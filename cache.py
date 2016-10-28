import hashlib
import redis

class CacheManager(object):
    def __init__(self, host, port, db):
        self.r = redis.StrictRedis(host=host, port=port, db=db)

    def gen_cachekey(self, namespace, raw_str):
        iso8859_1 = lambda key: key.encode('ISO8859-1') \
            if type(key) == unicode else key
        md5 = hashlib.md5(iso8859_1(raw_str))
        return namespace + ':' + md5.hexdigest()

    def put(self, query_str, result, expire=300):
        ck = self.gen_cachekey('dwapi:query', query_str)
        ret = None
        try:
            ret = self.r.setex(ck, expire, result)
        except Exception as e:
            print str(e)
        return ret

    def get(self, query_str):
        ck = self.gen_cachekey('dwapi:query', query_str)
        ret = None
        try:
            ret = self.r.get(ck)
        except Exception as e:
            print str(e)
        return ret

    def flushall(self):
        return self.r.flushall()

