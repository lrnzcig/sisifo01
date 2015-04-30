'''
Created on 29 de abr. de 2015

@author: lorenzorubio
'''
import unittest
import os
import pandas as pd
from nltk.twitter.util import guess_path, json2csv
from schema_aux import twitter_schema as sch
from datetime import datetime
from nltk.compat import UTC

def load_all_entities(path, filename):
    #tweet_loader(path, filename)
    user_loader(path, filename)

def delete_all_entities():
    manager = sch.Manager()
    session = manager.get_session()
    manager.delete_all()
    session.commit()

def tweet_loader(path, filename):
    json2csv(os.path.join(path, filename), 
             os.path.join(path, 'temp.csv'),
             ['created_at', 'favorite_count', 'id', 'in_reply_to_status_id', 'in_reply_to_user_id', 'retweet_count',
              'retweeted', 'text', 'truncated', {'user' : {'id'}}])
    
    tweets = pd.DataFrame.from_csv(os.path.join(path, 'temp.csv'), index_col=2, header=None)
    manager = sch.Manager()
    session = manager.get_session()
    for tweet in tweets.iterrows():
        created_at = datetime.strptime(tweet[1][0], '%a %b %d %H:%M:%S +0000 %Y').replace(tzinfo=UTC)
        tweet_id = tweet[0]
        old_tweet = False
        for row in session.query(sch.Tweet).filter(sch.Tweet.id == tweet_id).all():
            old_tweet = True
            if row.retweet_count < tweet[1][5]:
                # TODO check this is an update
                row.retweet_count = tweet[1][5]
            # query is by id, there's only 1 anyway
            break
        if old_tweet:
            continue
        tweet = sch.Tweet(created_at=created_at, favorite_count=tweet[1][1],
                          id=tweet_id, in_reply_to_status_id=tweet[1][3],
                          in_reply_to_user_id=tweet[1][4], retweet_count=tweet[1][5],
                          retweeted=tweet[1][6], text=tweet[1][7], truncated=tweet[1][8],
                          user_id=tweet[1][9])
        session.add(tweet)
    session.commit()

def user_loader(path, filename):
    json2csv(os.path.join(path, filename), 
             os.path.join(path, 'temp.csv'),
             [{'user' : ['id', 'created_at', 'contributors_enabled', 'description', 'favourites_count',
                         'followers_count', 'friends_count', 'is_translator', 'listed_count',
                         'location', 'name', 'protected', 'screen_name', 'statuses_count', 'url',
                         'verified']}])
    # no index since the same user might be two times in the file
    users = pd.DataFrame.from_csv(os.path.join(path, 'temp.csv'), index_col=None, header=None)
    manager = sch.Manager()
    session = manager.get_session()
    for user in users.iterrows():
        created_at = datetime.strptime(user[1][1], '%a %b %d %H:%M:%S +0000 %Y').replace(tzinfo=UTC)
        user_id = user[1][0]
        old_user = False
        description = user[1][3]
        if isinstance(description, float):
            description = ''
        location = user[1][9]
        if isinstance(location, float):
            location = ''
        url = user[1][14]
        if isinstance(url, float):
            url = ''
        for row in session.query(sch.User).filter(sch.User.id == user_id).all():
            old_user = True
            if row.statuses_count > user[1][12]:
                # assumed: if the user twitted more, there could be modifications
                row.contributors_enabled = user[1][2]
                row.description = description
                row.favourites_count = user[1][4]
                row.followers_count = user[1][5]
                row.friends_count = user[1][6]
                row.is_translator = user[1][7]
                row.listed_count = user[1][8]
                row.location = location
                row.name = user[1][10]
                row.protected = user[1][11]
                row.statuses_count = user[1][13]
                row.url = url
                row.verified = user[1][15]
            break
        if old_user:
            continue
        user = sch.User(id=user_id, screen_name=user[1][12], created_at=created_at,
                        contributors_enabled=user[1][2], description=description,
                        favourites_count = user[1][4], followers_count = user[1][5],
                        friends_count = user[1][6], is_translator = user[1][7],
                        listed_count = user[1][8], location = location, name = user[1][10],
                        protected = user[1][11], statuses_count = user[1][13], url=url,
                        verified = user[1][15])
        session.add(user)
    session.commit()

class Test(unittest.TestCase):

    def test_tweet_loader(self):
        delete_all_entities()
        load_all_entities(guess_path("twitter-files"), "tweets.20150429-192503.streaming-sample.json")
        pass


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.test_tweet_loader']
    unittest.main()