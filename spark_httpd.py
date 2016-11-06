import tornado.ioloop
import tornado.web
import tornado.gen
from tornado.options import define, options
from tornado.concurrent import run_on_executor
from concurrent.futures import ThreadPoolExecutor
from pyhocon import ConfigFactory
from cache import CacheManager
from db import db_session, ApiUsage

import os
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
spark = SparkSession.builder.master("yarn").appName("SparkSQL-HTTP").enableHiveSupport().getOrCreate()

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
    
def cache_key_gen(request,query):
    return query

def get_cache_option(request):
    cache = request.get_argument('cache', '')
    cache = cache.lower()
    return cache if cache in (CACHE_NO, CACHE_RENEW) else ''

DT_HANDLER = lambda obj: obj.strftime('%Y-%m-%d %H:%M:%S %f') \
    if isinstance(obj, dt.datetime) or isinstance(obj, dt.date) else None
    
import datetime as dt
import json
from pyspark.sql import Row
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
        self.resonse_fmt = ''
        super(SqlHandler, self).__init__(application, request, **kwargs)
    
    @tornado.web.asynchronous
    @tornado.gen.coroutine    
    def process_query(self, post=False):
        is_valid, uid = verify_token(self)
        print is_valid,uid
        self.info.update({'ctime': dt.datetime.now(),'uri': self.request.uri,'uid': uid,'hit_cache': False})
        if not is_valid:
            self.set_status(401)
            self.info.update({'elapsed': 0,
                              'status': 401,
                              'msg': ''})
            self.finish("Your request is not authorized.")
        else:
            try:
                query = None
                query = json.loads(self.request.body)['q'] if post else self.get_argument('q', '')
            except (ValueError, KeyError) as e:
                self.set_status(400)
                self.info.update({'elapsed': 0,'status': self.get_status(),'msg': str(e)})
            if not query:
                self.finish("Your request does not contains a valid query.")
            else:
                begin = dt.datetime.now()
                cache_key = cache_key_gen(self,query)
                cache_opt = get_cache_option(self)                                
                user = self.request.headers.get('user')
                namespace = 'dwapi:query:' + user if user else 'dwapi:query'
                fmt = self.get_argument('fmt', '')
                if fmt:
                    namespace = namespace + ":" + fmt
                    self.resonse_fmt = fmt
                    
                if cache_opt == CACHE_NO:
                    res = yield self.run_query(query)
                elif appcfg['api.cache.enabled'] and cache_opt == CACHE_RENEW:
                    res = yield self.run_query(query)
                    cm.put(cache_key, json.dumps(res, default=DT_HANDLER),namespace,appcfg['api.cache.expire'])
                elif appcfg['api.cache.enabled'] and not cache_opt:
                    res = cm.get(cache_key,namespace)
                    if not res:
                        res = yield self.run_query(query)
                        cm.put(cache_key, json.dumps(res, default=DT_HANDLER),namespace,appcfg['api.cache.expire'])
                    else:
                        res = json.loads(res)
                        self.info['hit_cache'] = True
                else:
                    res = yield self.run_query(query)
                elapsed = dt.datetime.now() - begin
                res['elapsed'] = elapsed.total_seconds()
                self.info.update({'elapsed': res['elapsed'],'status': 200,'msg': ''})
                self.set_header("Content-Type", "application/json")
                
                #cls = None if self.resonse_fmt.lower() == 'pretty' else CJsonEncoder
                self.write(json.dumps(res,cls=CJsonEncoder))
                self.finish()
    
    def get(self):
        self.process_query(False)
    
    def post(self):
        self.process_query(True)
    
        
    @run_on_executor
    def run_query(self, query):
        limited_query = self.add_limit(query)
        logging.info('Run spark query [%s]'% limited_query)
        ds = spark.sql(limited_query)
        if self.resonse_fmt.lower() == 'pretty':
            results = ds.toJSON().collect()
            res = {'result': [json.loads(x) for x in results]}
        else:
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
def load_to_rdd(sql):
    ds = spark.sql(sql)
    if appcfg['spark.repartition']:
        ds = ds.repartition(appcfg['spark.part_num'])
    ds.persist(StorageLevel(True, True, False, False))
    return ds
        
def register_temp_tables():
    for db in appcfg['data_source']:
        tables = []
        sql = "show tables in %s" % db['db']
        for table in spark.sql(sql).collect():
            if table['tableName'] not in db["tables_excluded"] and not table['isTemporary']:
                tables.append(table['tableName'])
            
        for table in tables:
            ds = load_to_rdd('select * from %s.%s'% (db['db'],table))
            temp_table_name ="%s_%s"% (db['db'],table)
            DFS.update({temp_table_name: ds})
            ds.createOrReplaceTempView(temp_table_name)
        
        for customized in db['tables_customized']:
            ds = load_to_rdd(customized['sql'])
            temp_table_name = temp_table_name ="%s_%s"% (db['db'],table)
            DFS.update({temp_table_name: ds})
            ds.createOrReplaceTempView(temp_table_name)
        
        

app = tornado.web.Application(handlers=[
    (r'/dw/api/sql?', SqlHandler)], 
    static_path=os.path.join(os.path.dirname(__file__), "templates"))

define("port", default=8888, help="run on the given port", type=int)

import logging
if __name__ == "__main__":    
    logging.basicConfig(level = appcfg['logging.level'],format=appcfg['logging.logfmt'], datefmt=appcfg['logging.datefmt'],
                        filename=appcfg['logging.filename'],filemode=appcfg['logging.filemode'])
    
    register_temp_tables()
    print "Registered tables:"
    print DFS.keys()
    tornado.options.parse_command_line()
    app.listen(options.port)
    print "Data warehouse API server running at port %s" % options.port
    tornado.ioloop.IOLoop.instance().start()