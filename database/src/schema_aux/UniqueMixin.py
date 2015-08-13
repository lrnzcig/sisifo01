'''
Created on 5 de may. de 2015

@from: https://bitbucket.org/zzzeek/sqlalchemy/wiki/UsageRecipes/UniqueObject

modified for implementing a cache for not UniqueMixin objects
'''
from schema_aux import CachedMixin

def _unique(session, cache, cls, hashfunc, queryfunc, constructor, arg, kw):
    '''
    1. tries to find the object in cache (the cache will create a new one if
        it does not exist) 
    2. if not in cache, it creates a new object and adds it to the session
        (and to the cache of course)
    '''
    obj = CachedMixin._get_from_cache(session, cache, cls, hashfunc, queryfunc, arg, kw)
    if not obj:
        obj = constructor(*arg, **kw)
        session.add(obj)
        CachedMixin._add_to_cache(session, cache, cls, hashfunc, arg, kw, obj)
    return obj

    
class UniqueMixin(object):
    @classmethod
    def unique_hash(cls, *arg, **kw):
        raise NotImplementedError()

    @classmethod
    def unique_filter(cls, query, *arg, **kw):
        raise NotImplementedError()

    @classmethod
    def as_unique(cls, session, cache, *arg, **kw):
        return _unique(
                    session,
                    cache,
                    cls,
                    cls.unique_hash,
                    cls.unique_filter,
                    cls,
                    arg, kw
               )
        