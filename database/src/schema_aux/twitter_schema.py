'''
Created on 15 de abr. de 2015

@author: lorenzorubio
'''
from sqlalchemy import Column, Integer, String, Date, SmallInteger, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import schema_aux.utils as utils
from schema_aux.UniqueMixin import UniqueMixin
from schema_aux.CachedMixin import CachedMixin

Base = declarative_base()

class Tweet(CachedMixin, Base):
    __tablename__ = 'tweet'
    created_at = Column(Date)
    favorite_count = Column(Integer)
    id = Column(Integer, primary_key=True)    
    in_reply_to_status_id = Column(Integer) 
    in_reply_to_user_id = Column(Integer)
    place_full_name = Column(String(256))
    retweet_count = Column(Integer)
    retweet = Column(SmallInteger)
    retweeted_id = Column(Integer)
    retweeted_user_id = Column(Integer)
    text = Column(String(1024))
    truncated = Column(SmallInteger)
    user_id = Column(Integer)

    def __repr__(self):
        return "<tweet(id='%s', text='%s', user_id='%s')>" % (
            self.id, self.text, self.user_id)

    @classmethod
    def unique_hash(cls, *arg, **kw):
        return str(kw['id'])

    @classmethod
    def unique_filter(cls, query, *arg, **kw):
        return query.filter(Tweet.id == kw['id'])

class User(CachedMixin, Base):
    __tablename__ = 'tuser'
    created_at = Column(Date)
    contributors_enabled = Column(SmallInteger)
    description = Column(String(1024))
    favourites_count = Column(Integer)
    followers_count = Column(Integer)
    friends_count = Column(Integer)
    id = Column(Integer, primary_key=True)    
    is_translator = Column(SmallInteger)
    listed_count = Column(Integer)
    location = Column(String(256))
    name = Column(String(256))
    protected = Column(SmallInteger)
    screen_name = Column(String(256))
    statuses_count = Column(Integer)
    url = Column(String(1024))
    verified = Column(SmallInteger)
    withheld = Column(SmallInteger)
    profile_link_color = Column(String(6))

    def __repr__(self):
        return "<tuser(id='%s', screen_name='%s')>" % (
            self.id, self.screen_name)

    @classmethod
    def unique_hash(cls, *arg, **kw):
        return str(kw['id'])

    @classmethod
    def unique_filter(cls, query, *arg, **kw):
        return query.filter(User.id == kw['id'])

class Hashtag(UniqueMixin, Base):
    '''
    May be duplicated in the input file, however only one instance
    makes sense since the info is not dynamic
    Create objects with as_unique (inherited from UniqueMixin)
    '''
    __tablename__ = 'thashtag'
    tweet_id = Column(Integer, primary_key=True)
    hashtag = Column(String(256), primary_key=True)
    
    def __repr__(self):
        return "<thashtag(tweet_id='%s')>" % self.tweet_id
    
    @classmethod
    def unique_hash(cls, tweet_id, hashtag):
        return hash(str(tweet_id) + hashtag)

    @classmethod
    def unique_filter(cls, query, tweet_id, hashtag):
        return query.filter(Hashtag.tweet_id == tweet_id, Hashtag.hashtag == hashtag)

class TweetUrl(UniqueMixin, Base):
    '''
    May be duplicated in the input file, however only one instance
    makes sense since the info is not dynamic
    Create objects with as_unique (inherited from UniqueMixin)
    '''
    __tablename__ = 'turl'
    tweet_id = Column(Integer, primary_key=True)
    url = Column(String(1024), primary_key=True)
    
    def __repr__(self):
        return "<turl(tweet_id='%s')>" % self.tweet_id

    @classmethod
    def unique_hash(cls, tweet_id, url):
        return hash(str(tweet_id) + url)

    @classmethod
    def unique_filter(cls, query, tweet_id, url):
        return query.filter(TweetUrl.tweet_id == tweet_id, TweetUrl.url == url)

class UserMention(UniqueMixin, Base):
    '''
    May be duplicated in the input file, however only one instance
    makes sense since the info is not dynamic
    Create objects with as_unique (inherited from UniqueMixin)
    '''
    __tablename__ = 'tusermention'
    tweet_id = Column(Integer, primary_key=True)
    source_user_id = Column(Integer, primary_key=True)
    target_user_id = Column(Integer, primary_key=True)
    
    def __repr__(self):
        return "<tusermention(tweet_id='%s', source_user_id='%s', target_user_id='%s')>" % ( 
            self.tweet_id, self.source_user_id, self.target_user_id)

    @classmethod
    def unique_hash(cls, tweet_id, source_user_id, target_user_id):
        return hash(str(tweet_id) + str(source_user_id) + str(target_user_id))

    @classmethod
    def unique_filter(cls, query, tweet_id, source_user_id, target_user_id):
        return query.filter(UserMention.tweet_id == tweet_id,
                            UserMention.source_user_id == source_user_id,
                            UserMention.target_user_id == target_user_id)

class UserUrl(UniqueMixin, Base):
    '''
    May be duplicated in the input file, however only one instance
    makes sense since the info is not dynamic
    Create objects with as_unique (inherited from UniqueMixin)
    '''
    __tablename__ = 'tuserurl'
    user_id = Column(Integer, primary_key=True)
    url = Column(String(1024), primary_key=True)
    
    def __repr__(self):
        return "<tuserurl(user_id='%s')>" % self.user_id
    
    @classmethod
    def unique_hash(cls, user_id, url):
        return hash(str(user_id) + url)

    @classmethod
    def unique_filter(cls, query, user_id, url):
        return query.filter(UserUrl.user_id == user_id, UserUrl.url == url)


class Follower(UniqueMixin, Base):
    '''
    May be duplicated in the input file, however only one instance
    makes sense since the info is not dynamic
    Create objects with as_unique (inherited from UniqueMixin)
    '''
    __tablename__ = 'follower'
    user_id = Column(Integer, primary_key=True)
    followed_user_id = Column(Integer, primary_key=True)
    start_date = Column(Date)
    end_date = Column(Date)
    
    def __repr__(self):
        return "<follower(user_id='%s', followed_user_id='%s')>" % (
            self.user_id, self.followed_user_id)

    @classmethod
    def unique_hash(cls, user_id, followed_user_id):
        return hash(str(user_id) + str(followed_user_id))

    @classmethod
    def unique_filter(cls, query, user_id, followed_user_id):
        return query.filter(Follower.user_id == user_id,
                            Follower.followed_user_id == followed_user_id)


class Manager():
    '''
    schema manager
    '''
    
    def __init__(self, user=None, alchemy_echo=True):
        url = utils.get_database_url_sql_alchemy(user, alchemy_echo)
        self.engine = create_engine(url, echo=alchemy_echo)
        Base.metadata.create_all(self.engine)

    def get_session(self):
        Session = sessionmaker(bind=self.engine)
        return Session()

    def delete_all(self):
        session = self.get_session()
        session.query(Tweet).delete()
        session.query(User).delete()
        session.query(Hashtag).delete()
        session.query(TweetUrl).delete()
        session.query(UserMention).delete()
        session.query(UserUrl).delete()
        session.query(Follower).delete()
        session.commit()
