'''
Created on 29 de abr. de 2015

@author: lorenzorubio
'''
import unittest
import os
import pandas as pd
from nltk.twitter.util import guess_path, json2csv, json2csv_entities
from schema_aux import twitter_schema as sch
from datetime import datetime
from nltk.compat import UTC

def load_all_entities(path, filename):
    tweet_loader(path, filename)
    user_loader(path, filename)
    hashtag_loader(path, filename)
    tweet_url_loader(path, filename)
    user_mention_loader(path, filename)
    user_url_loader(path, filename)

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
        old_tweet = sch.Tweet.get(session, id=tweet_id)
        if old_tweet:
            if old_tweet.retweet_count < tweet[1][5] or old_tweet.favorite_count < tweet[1][1]:
                # it is more recent
                old_tweet.retweet_count = tweet[1][5]
                old_tweet.favorite_count = tweet[1][1]
                #sch.Tweet.invalidate(session, tweet_id=tweet_id)
            continue
        tweet = sch.Tweet.as_cached(session, created_at=created_at, favorite_count=tweet[1][1],
                          id=tweet_id, in_reply_to_status_id=tweet[1][3],
                          in_reply_to_user_id=tweet[1][4], retweet_count=tweet[1][5],
                          retweeted=tweet[1][6], text=tweet[1][7], truncated=tweet[1][8],
                          user_id=tweet[1][9])
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
    # pandas reads as nan even for string columns => fill them with empty string
    users[3].fillna('', inplace=True)
    users[9].fillna('', inplace=True)
    users[14].fillna('', inplace=True)
    manager = sch.Manager()
    session = manager.get_session()
    for user in users.iterrows():
        created_at = datetime.strptime(user[1][1], '%a %b %d %H:%M:%S +0000 %Y').replace(tzinfo=UTC)
        user_id = user[1][0]
        old_user = sch.User.get(session, id=user_id)
        if old_user:
            if old_user.statuses_count < user[1][13]:
                # assumed: if the user twitted more, there could be modifications
                old_user.contributors_enabled = user[1][2]
                old_user.description = user[1][3]
                old_user.favourites_count = user[1][4]
                old_user.followers_count = user[1][5]
                old_user.friends_count = user[1][6]
                old_user.is_translator = user[1][7]
                old_user.listed_count = user[1][8]
                old_user.location = user[1][9]
                old_user.name = user[1][10]
                old_user.protected = user[1][11]
                old_user.statuses_count = user[1][13]
                old_user.url = user[1][14]
                old_user.verified = user[1][15]
            continue
        user = sch.User.as_cached(session, id=user_id, screen_name=user[1][12], created_at=created_at,
                        contributors_enabled=user[1][2], description=user[1][3],
                        favourites_count = user[1][4], followers_count = user[1][5],
                        friends_count = user[1][6], is_translator = user[1][7],
                        listed_count = user[1][8], location = user[1][9], name = user[1][10],
                        protected = user[1][11], statuses_count = user[1][13], url=user[1][14],
                        verified = user[1][15])
    session.commit()

def hashtag_loader(path, filename):
    json2csv_entities(os.path.join(path, filename), 
                      os.path.join(path, 'temp.csv'),
                      ['id'], 'hashtags', ['text'])
    hashtags = pd.DataFrame.from_csv(os.path.join(path, 'temp.csv'), index_col=0, header=None)
    manager = sch.Manager()
    session = manager.get_session()
    for hashtag in hashtags.iterrows():
        hashtag = sch.Hashtag.as_unique(session, tweet_id=hashtag[0], hashtag=hashtag[1][1])
    session.commit()

def tweet_url_loader(path, filename):
    json2csv_entities(os.path.join(path, filename), 
                      os.path.join(path, 'temp.csv'),
                      ['id'], 'urls', ['url'])
    tweeturls = pd.DataFrame.from_csv(os.path.join(path, 'temp.csv'), index_col=None, header=None)
    manager = sch.Manager()
    session = manager.get_session()
    for tweeturl in tweeturls.iterrows():
        tweeturl = sch.TweetUrl.as_unique(session, tweet_id=tweeturl[1][0], url=tweeturl[1][1])
        session.add(tweeturl)
    session.commit()  
        
def user_mention_loader(path, filename):
    json2csv_entities(os.path.join(path, filename), 
                      os.path.join(path, 'temp.csv'),
                      ['id'], 'user_mentions', ['id'])
    usermentions = pd.DataFrame.from_csv(os.path.join(path, 'temp.csv'), index_col=None, header=None)
    manager = sch.Manager()
    session = manager.get_session()
    for usermention in usermentions.iterrows():
        tweet_id = usermention[1][0]
        source_user_id = None
        for row in session.query(sch.Tweet).filter(sch.Tweet.id == tweet_id).all():
            source_user_id = row.user_id
            break
        if source_user_id == None:
            raise RuntimeError("Incongruent data - there's user mentions for a tweet that does not exist. Tweet id: " + str(tweet_id))
        usermention = sch.UserMention.as_unique(session, tweet_id=tweet_id, source_user_id=source_user_id, 
                                      target_user_id=usermention[1][1])
        session.add(usermention)
    session.commit()  

def user_url_loader(path, filename):
    temp_file = os.path.join(path, 'temp.csv')
    json2csv_entities(os.path.join(path, filename), 
                      temp_file,
                      ['id'], {'user' : 'urls'}, ['url'])
    if os.stat(temp_file).st_size == 0:
        # there are no user urls, not at all uncommon
        return
    userurls = pd.DataFrame.from_csv(temp_file, index_col=None, header=None)
    manager = sch.Manager()
    session = manager.get_session()
    for userurl in userurls.iterrows():
        userurl = sch.TweetUrl.as_unique(session, tweet_id=userurl[1][0], url=userurl[1][1])
        session.add(userurl)
    session.commit()  
    
class Test(unittest.TestCase):

    def test_tweet_loader(self):
        delete_all_entities()
        load_all_entities(guess_path("twitter-files"), "tweets.20150429-192503.streaming-sample.json")
        pass


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.test_tweet_loader']
    unittest.main()