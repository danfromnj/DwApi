import tornado.ioloop
import tornado.web
import tornado.gen
from tornado import escape
from tornado.options import define, options
from tornado.concurrent import run_on_executor
from concurrent.futures import ThreadPoolExecutor
from pyhocon import ConfigFactory
from cache import CacheManager
from db import db_session, ApiUsage

import os
from pip._vendor.requests.utils import is_valid_cidr
paths = [os.environ["SPARK_HOME"] + '/python',
         os.environ["SPARK_HOME"] + '/bin',
         os.environ["SPARK_HOME"] + '/python/lib/py4j-0.10.3-src.zip']


import sys
for path in paths:
    sys.path.append(path)
    
appcfg = ConfigFactory.parse_file(os.environ['DWAPI_CONF'])
cm = CacheManager(host=appcfg['redis.host'],
                  port=appcfg['redis.port'],
                  db=appcfg['redis.db'])
CACHE_NO = 'no'
CACHE_RENEW = 'renew'

from pyspark import StorageLevel
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("yarn").appName("SimplePythonClient").enableHiveSupport().getOrCreate()

import platform
INFO_TMPL = {'srv': platform.uname()[1], 'status': 200, 'uri': '',
             'db': '', 'tab': '', 'ctime': None, 'uid': '',
           'hit_cache': False, 'elapsed': 0, 'msg': ''}

import jwt
def verify_token(request_handler):
    try:
        encoded = request_handler.request.headers.get('X-DWAPI-Token')
        token = jwt.decode(encoded, appcfg['secret'], algorithm='HS256')
        if token['uid'] in appcfg['api.allowed']:
            return True, token['uid']
        else:
            return False, ''
    except AttributeError:
        return False, ''
    except jwt.exceptions.DecodeError:
        return False, ''
    
def cache_key_gen(request):
    keys = (
        request.get_argument('db', ''),
        request.get_argument('table', ''),
        request.get_argument('q', ''),
        request.get_argument('limit', ''))
    return '-*-'.join(keys)

def get_cache_option(request):
    cache = request.get_argument('cache', '')
    cache = cache.lower()
    return cache if cache in (CACHE_NO, CACHE_RENEW) else ''

DT_HANDLER = lambda obj: obj.strftime('%Y-%m-%d %H:%M:%S %f') \
    if isinstance(obj, dt.datetime) or isinstance(obj, dt.date) else None
    
import datetime as dt
import json

class CJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, dt.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S.%f')
        elif isinstance(obj, dt.date):
            return obj.strftime('%Y-%m-%d')
        else:
            return json.JSONEncoder.default(self, obj)
        
class SqlHandler(tornado.web.RequestHandler):
    executor = ThreadPoolExecutor(max_workers=appcfg['server.max_workers'])
    def __init__(self, application, request, **kwargs):
        self.info = INFO_TMPL
        super(SqlHandler, self).__init__(application, request, **kwargs)
    
    def preprocess_query(self, post=False):
        is_valid, uid = verify_token(self)
        if not is_valid:
            self.set_status(401)
            self.info.update({'elapsed': 0,
                              'status': 401,
                              'msg': ''})
            return False,uid,None
        
        return is_valid, uid, self.request.body if post else self.get_argument('q', '')
    
    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def get(self):
        uri = self.request.uri
        is_valid, uid = verify_token(self)
        self.info.update({'ctime': dt.datetime.now(),
                          'uri': uri,
                          'uid': uid,
                          'hit_cache': False})

        if not is_valid:
            self.set_status(401)
            self.info.update({'elapsed': 0,
                              'status': 401,
                              'msg': ''})
            self.finish("Your request is not authorized.")
        else:
            begin = dt.datetime.now()
            cache_key = cache_key_gen(self)
            query = self.get_argument('q', '')
            cache_opt = get_cache_option(self)
            if cache_opt == CACHE_NO:
                res = yield self.run_query(query)
            elif appcfg['api.cache.enabled'] and cache_opt == CACHE_RENEW:
                res = yield self.run_query(query)
                cm.put(cache_key, json.dumps(res, default=DT_HANDLER),
                       appcfg['api.cache.expire'])
            elif appcfg['api.cache.enabled'] and not cache_opt:
                res = cm.get(cache_key)
                if not res:
                    res = yield self.run_query(query)
                    cm.put(cache_key, json.dumps(res, default=DT_HANDLER),
                           appcfg['api.cache.expire'])
                else:
                    res = json.loads(res)
                    self.info['hit_cache'] = True
            else:
                res = yield self.run_query(query)
            elapsed = dt.datetime.now() - begin
            res['elapsed'] = elapsed.total_seconds()
            self.info.update({'elapsed': res['elapsed'],
                              'status': 200,
                              'msg': ''})
            self.set_header("Content-Type", "application/json")
            self.write(json.dumps(res,cls=CJsonEncoder))
            self.finish()
    
    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def post(self):
        print self.request.body
        
    @run_on_executor
    def run_query(self, query):
        limited_query = self.add_limit(query)
        ds = spark.sql(limited_query)
        #results = ds.toJSON().collect()
        #res = {'result': [json.loads(x) for x in results]}
        results = ds.collect()
        res = {'result': results}
        return res
    
    def add_limit(self, query):
        words = query.split()
        size = len(words)
        if size  == 0 or not words[0].lower().startswith('select'):
            return query
            
        if size < 2 or words[size - 2].lower() != 'limit':
            query = query + ' limit 1024'
        elif size >= 2:
            try:
                limit = int(words[size - 1])
                if limit > 1024:
                    query = query[ :query.rfind(words[size-1])] + '1024'
            except Exception:
                pass
        return query

    def write_error(self, status_code, **kwargs):
        self.info.update({'elapsed': 0,
                          'status': status_code,
                          'msg': str(kwargs["exc_info"])})

    def on_finish(self):
        au = ApiUsage(**self.info)
        db_session.add(au)
        db_session.commit()


DFS = {}
def load_df(_table):
    def load_df_from_hive(table):
        hql_tmpl = lambda hive_db, tname: 'FROM %s.%s SELECT *' % (hive_db, tname)
        hive_sql = table['sql'] if 'sql' in table.keys() \
            else hql_tmpl(table['hive_db'], table['name'])
        df1 = spark.sql(hive_sql)
        return df1

    def load_df_from_inline_data(table):
        return spark.createDataFrame(table['data'], table['cols'])

    load_from = {
        'hive': lambda table: load_df_from_hive(table)
    }
    df = load_from[_table['src']](_table)
    if appcfg['spark.repartition']:
        df = df.repartition(appcfg['spark.part_num'])
    return df

def prepare(_db, _table):
    table_name = '{db}_{table}'.format(db=_db['name'], table=_table['name'])
    df = load_df(_table)
    df.persist(StorageLevel(True, True, False, False))
    DFS.update({table_name: df})
    df.createOrReplaceTempView(table_name)

def register_tables():
    for db in appcfg['data_source']:
        for table in db['tables']:
            prepare(db, table)
    print "Registered tables:"
    print DFS.keys()

app = tornado.web.Application(handlers=[
    (r'/dw/api/sql?', SqlHandler)], 
    static_path=os.path.join(os.path.dirname(__file__), "templates"))

define("port", default=8888, help="run on the given port", type=int)

if __name__ == "__main__":
    register_tables()
    tornado.options.parse_command_line()
    app.listen(options.port)
    print "Data warehouse API server running at port %s" % options.port
    tornado.ioloop.IOLoop.instance().start()