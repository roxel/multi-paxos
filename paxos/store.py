import redis


class StoreMixin(object):
    """
    Provides base for persistent storing of key-value pairs.
    """

    def redis_connection(self):
        return redis.StrictRedis(host=self.redis_host, port=self.redis_port, db=self.id)

    def set(self, key, value):
        r = self.redis_connection()
        result = r.set(key, value)
        return result

    def get(self, key):
        r = self.redis_connection()
        result = r.get(key)
        return result
