'''
Created on 15 de abr. de 2015

@author: lorenzorubio
'''
from sqlalchemy import Column, Integer, String, Date, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from os.path import expanduser
import yaml

Base = declarative_base()

class Tweet(Base):
    __tablename__ = 'tweet'
    created_at = Column(Date)
    favorite_count = Column(Integer)
    id = Column(Integer, primary_key=True)    
    in_reply_to_status_id = Column(Integer) 
    in_reply_to_user_id = Column(Integer)
    place_full_name = Column(String(256))
    retweet_count = Column(Integer)
    retweeted = Column(String(1))
    retweeted_id = Column(Integer)
    text = Column(String(256))
    truncated = Column(String(1))
    user_id = Column(Integer)

    def __repr__(self):
        return "<Tweet(id='%s', text='%s', user_id='%s')>" % (
            self.id, self.text, self.user_id)

class User(Base):
    __tablename__ = 'tuser'
    created_at = Column(Date)
    contributors_enabled = Column(String(1))
    description = Column(String(1024))
    favourites_count = Column(Integer)
    followers_count = Column(Integer)
    friends_count = Column(Integer)
    id = Column(Integer, primary_key=True)    
    is_translator = Column(String(1))
    listed_count = Column(Integer)
    location = Column(String(256))
    name = Column(String(256))
    protected = Column(String(1))
    screen_name = Column(String(256))
    statuses_count = Column(Integer)
    url = Column(String(1024))
    verified = Column(String(1))
    withheld = Column(String(1))

    def __repr__(self):
        return "<Tuser(id='%s', screen_name='%s')>" % (
            self.id, self.screen_name)


class Manager():
    '''
    schema manager
    '''
    
    def __init__(self):
        # export PATH or pass as an argument!
        properties = yaml.load(open(expanduser("~") + '/.sisifo/connection.properties'))
        database = properties['database']
        url = database['dialect'] + "://" + database['user'] + ":" + database['password'] + '@' + database['host'] + "/" + database['sid']
        self.engine = create_engine(url, echo=True)
        Base.metadata.create_all(self.engine)

    def get_session(self):
        Session = sessionmaker(bind=self.engine)
        return Session()
