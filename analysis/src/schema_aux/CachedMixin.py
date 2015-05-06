'''
Created on 5 de may. de 2015

@author: lorenzorubio
'''

def _get_from_cache(session, cls, hashfunc, queryfunc, arg, kw):
    '''
    1. gets object from cache
    2. if it does not exist, returns None
    '''
    cache = _get_cache(session)

    key = (cls, hashfunc(*arg, **kw))
    if key in cache:
        return cache[key]
    else:
        with session.no_autoflush:
            q = session.query(cls)
            q = queryfunc(q, *arg, **kw)
            obj = q.first()
            if not obj:
                return None
        cache[key] = obj
        return obj

def _add_to_cache(session, cls, hashfunc, arg, kw, obj):
    cache = _get_cache(session)
    key = (cls, hashfunc(*arg, **kw))
    cache[key] = obj

def _get_cache(session):
    cache = getattr(session, '_unique_cache', None)
    if cache is None:
        session._unique_cache = cache = {}
    return cache

    
class CachedMixin(object):
    @classmethod
    def unique_hash(cls, *arg, **kw):
        raise NotImplementedError()

    @classmethod
    def unique_filter(cls, query, *arg, **kw):
        raise NotImplementedError()

    @classmethod
    def get(cls, session, *arg, **kw):
        '''
        gets object from the cache/ddbb, does not create if it does not exist 
        '''
        return _get_from_cache(
                    session,
                    cls,
                    cls.unique_hash,
                    cls.unique_filter,
                    arg, kw
               )

    @classmethod
    def as_cached(cls, session, *arg, **kw):
        '''
        create object and add to the cache
        '''
        obj = cls(*arg, **kw)
        session.add(obj)
        _add_to_cache(session, cls, cls.unique_hash, arg, kw, obj)
    
