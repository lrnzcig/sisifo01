'''
Created on 5 de may. de 2015

@author: lorenzorubio
'''

def _get_from_cache(session, cache, cls, hashfunc, queryfunc, arg, kw):
    '''
    1. gets object from cache
    2. if it does not exist, returns None
    '''
    key = None
    if cache != None:
        key = (cls, hashfunc(*arg, **kw))
    if key != None and key in cache:
        # in case object has been flushed
        obj = cache[key]
        session.add(obj)
        return obj
    else:
        with session.no_autoflush:
            q = session.query(cls)
            q = queryfunc(q, *arg, **kw)
            obj = q.first()
            if not obj:
                return None
        if cache != None:
            cache[key] = obj
        session.add(obj)
        return obj

def _add_to_cache(session, cache, cls, hashfunc, arg, kw, obj):
    key = (cls, hashfunc(*arg, **kw))
    if cache != None:
        cache[key] = obj

class CachedMixin(object):
    
    @classmethod
    def unique_hash(cls, *arg, **kw):
        raise NotImplementedError()

    @classmethod
    def unique_filter(cls, query, *arg, **kw):
        raise NotImplementedError()

    @classmethod
    def get(cls, session, cache, *arg, **kw):
        '''
        gets object from the cache/ddbb, does not create if it does not exist 
        '''
        return _get_from_cache(
                    session,
                    cache,
                    cls,
                    cls.unique_hash,
                    cls.unique_filter,
                    arg, kw
               )

    @classmethod
    def as_cached(cls, session, cache, *arg, **kw):
        '''
        create object and add to the cache
        '''
        obj = cls(*arg, **kw)
        session.add(obj)
        _add_to_cache(session, cache, cls, cls.unique_hash, arg, kw, obj)

    
