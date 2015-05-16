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
    # one session for the full load to make use of the cache
    manager = sch.Manager()
    session = manager.get_session()

    tweet_loader(path, filename, session, regs_per_commit=100)
    user_loader(path, filename, session, regs_per_commit=100)
    hashtag_loader(path, filename, session, regs_per_commit=100)
    tweet_url_loader(path, filename, session, regs_per_commit=100)
    user_mention_loader(path, filename, session, regs_per_commit=100)
    user_url_loader(path, filename, session, regs_per_commit=100)

def delete_all_entities():
    manager = sch.Manager()
    session = manager.get_session()
    manager.delete_all()
    session.commit()

def tweet_loader(path, filename, session, regs_per_commit=None):
    json2csv(os.path.join(path, filename), 
             os.path.join(path, 'temp.csv'),
             ['created_at', 'favorite_count', 'id', 'in_reply_to_status_id', 'in_reply_to_user_id', 'retweet_count',
              'text', 'truncated', {'user' : {'id'}}])
    
    tweets = pd.DataFrame.from_csv(os.path.join(path, 'temp.csv'), index_col=2, header=None)
    
    json2csv_entities(os.path.join(path, filename), 
                      os.path.join(path, 'temp2.csv'),
                      ['id'], 'retweeted_status', ['created_at', 'favorite_count', 'id', 'in_reply_to_status_id', 'in_reply_to_user_id', 'retweet_count',
                                                   'text', 'truncated', {'user' : {'id'}}])
    
    orig_tweets = pd.DataFrame.from_csv(os.path.join(path, 'temp2.csv'), index_col=0, header=None)
    
    counter = 0
    for tweet in tweets.iterrows():
        created_at = datetime.strptime(tweet[1][0], '%a %b %d %H:%M:%S +0000 %Y').replace(tzinfo=UTC)
        tweet_id = tweet[0]
        old_tweet = sch.Tweet.get(session, id=tweet_id)
        if old_tweet:
            if old_tweet.retweet_count < tweet[1][5] or old_tweet.favorite_count < tweet[1][1]:
                # it is more recent
                old_tweet.retweet_count = tweet[1][5]
                old_tweet.favorite_count = tweet[1][1]
            continue
        retweet = False
        retweeted_id = None
        retweeted_user_id = None
        try:
            original_tweet_df = orig_tweets.loc[tweet_id]
        except KeyError:
            original_tweet_df = []
        if len(original_tweet_df) != 0:
            # this is a retweet
            retweet = True
            original_tweet_id = original_tweet_df[3]
            original_tweet = sch.Tweet.get(session, id=original_tweet_id)
            if not original_tweet:
                # insert it already
                orig_created_at = datetime.strptime(original_tweet_df[1], '%a %b %d %H:%M:%S +0000 %Y').replace(tzinfo=UTC)
                truncated = False
                if original_tweet_df[8]:
                    # the original is numpy.bool_ and SQLAlchemy does not accept it
                    truncated = True
                sch.Tweet.as_cached(session, created_at=orig_created_at, favorite_count=original_tweet_df[2],
                                    id=original_tweet_id, in_reply_to_status_id=original_tweet_df[4],
                                    in_reply_to_user_id=original_tweet_df[5], retweet_count=original_tweet_df[6],
                                    retweet=False, text=original_tweet_df[7], truncated=truncated,
                                    user_id=original_tweet_df[9], retweeted_id=None, retweeted_user_id=None)
            retweeted_id = original_tweet_id
            retweeted_user_id = original_tweet_df[9]
        sch.Tweet.as_cached(session, created_at=created_at, favorite_count=tweet[1][1],
                            id=tweet_id, in_reply_to_status_id=tweet[1][3],
                            in_reply_to_user_id=tweet[1][4], retweet_count=tweet[1][5],
                            retweet=retweet, text=tweet[1][6], truncated=tweet[1][7],
                            user_id=tweet[1][8], retweeted_id=retweeted_id, retweeted_user_id=retweeted_user_id)
        counter += 1
        if regs_per_commit and counter % regs_per_commit == 0:
            session.commit()
    session.commit()

def user_loader(path, filename, session, regs_per_commit=None):
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
    users[10].fillna('', inplace=True)
    users[14].fillna('', inplace=True)
    counter = 0
    for user in users.iterrows():
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
        created_at = datetime.strptime(user[1][1], '%a %b %d %H:%M:%S +0000 %Y').replace(tzinfo=UTC)
        user = sch.User.as_cached(session, id=user_id, screen_name=user[1][12], created_at=created_at,
                        contributors_enabled=user[1][2], description=user[1][3],
                        favourites_count = user[1][4], followers_count = user[1][5],
                        friends_count = user[1][6], is_translator = user[1][7],
                        listed_count = user[1][8], location = user[1][9], name = user[1][10],
                        protected = user[1][11], statuses_count = user[1][13], url=user[1][14],
                        verified = user[1][15])
        counter += 1
        if regs_per_commit and counter % regs_per_commit == 0:
            session.commit()
    session.commit()

def hashtag_loader(path, filename, session, regs_per_commit=None):
    json2csv_entities(os.path.join(path, filename), 
                      os.path.join(path, 'temp.csv'),
                      ['id'], 'hashtags', ['text'])
    hashtags = pd.DataFrame.from_csv(os.path.join(path, 'temp.csv'), index_col=0, header=None)
    counter = 0
    for hashtag in hashtags.iterrows():
        hashtag = sch.Hashtag.as_unique(session, tweet_id=hashtag[0], hashtag=hashtag[1][1])
        counter += 1
        if regs_per_commit and counter % regs_per_commit == 0:
            session.commit()
    session.commit()

def tweet_url_loader(path, filename, session, regs_per_commit=None):
    json2csv_entities(os.path.join(path, filename), 
                      os.path.join(path, 'temp.csv'),
                      ['id'], 'urls', ['url'])
    tweeturls = pd.DataFrame.from_csv(os.path.join(path, 'temp.csv'), index_col=None, header=None)
    counter = 0
    for tweeturl in tweeturls.iterrows():
        tweeturl = sch.TweetUrl.as_unique(session, tweet_id=tweeturl[1][0], url=tweeturl[1][1])
        counter += 1
        if regs_per_commit and counter % regs_per_commit == 0:
            session.commit()
    session.commit()  
        
def user_mention_loader(path, filename, session, regs_per_commit=None):
    json2csv_entities(os.path.join(path, filename), 
                      os.path.join(path, 'temp.csv'),
                      ['id'], 'user_mentions', ['id'])
    usermentions = pd.DataFrame.from_csv(os.path.join(path, 'temp.csv'), index_col=None, header=None)
    counter = 0
    for usermention in usermentions.iterrows():
        tweet_id = usermention[1][0]
        old_tweet = sch.Tweet.get(session, id=tweet_id)
        if old_tweet == None:
            raise RuntimeError("Incongruent data - there's user mentions for a tweet that does not exist. Tweet id: " + str(tweet_id))
        usermention = sch.UserMention.as_unique(session, tweet_id=tweet_id, source_user_id=old_tweet.user_id, 
                                      target_user_id=usermention[1][1])
        counter += 1
        if regs_per_commit and counter % regs_per_commit == 0:
            session.commit()
    session.commit()  

def user_url_loader(path, filename, session, regs_per_commit=None):
    temp_file = os.path.join(path, 'temp.csv')
    json2csv_entities(os.path.join(path, filename), 
                      temp_file,
                      ['id'], {'user' : 'urls'}, ['url'])
    if os.stat(temp_file).st_size == 0:
        # there are no user urls, not at all uncommon
        return
    userurls = pd.DataFrame.from_csv(temp_file, index_col=None, header=None)
    counter = 0
    for userurl in userurls.iterrows():
        userurl = sch.TweetUrl.as_unique(session, tweet_id=userurl[1][0], url=userurl[1][1])
        counter += 1
        if regs_per_commit and counter % regs_per_commit == 0:
            session.commit()
    session.commit()
    
def fill_in_retweet_info(session, regs_per_commit=None):
    q = session.query(sch.Tweet)
    tweets = pd.read_sql(q.statement, session.bind)
    counter = 0
    for tweet in tweets.iterrows():
        if tweet[1]['text'].startswith('RT @'):
            tmp = tweet[1]['text'].split('RT @')[1]
            screen_name = tmp.split(':')[0]
            try:
                tweet_text = tmp.split(screen_name + ': ')[1].rstrip('?') + '%'
            except IndexError:
                tweet_text = None
            if tweet_text:
                # TODO encoding problem
                # TODO these should go to cache as well, at least the ones that have been found ???
                original_tweet = session.query(sch.Tweet).filter(sch.Tweet.text.like(tweet_text.replace('?', '%'))).first()

            # TODO this is to put the tweet into the session 
            tweet = sch.Tweet.get(session, id=tweet[1]['id'])
            tweet.retweeeted = True
                            
            if original_tweet:
                tweet.retweeted_id = original_tweet.id
                tweet.retweeted_user_id = original_tweet.user_id
            else:
                # original tweet not found. Let's try with screen name
                # to at least inform the user
                user = sch.User.get(session, screen_name=screen_name)
                if user:
                    # read_sql does not put tweet in the session/cache
                    # TODO get all and add to cache???
                    tweet.retweeted_user_id = user.id
            counter += 1
            if regs_per_commit and counter % regs_per_commit == 0:
                session.commit()
    session.commit()
                
    
class Test(unittest.TestCase):

    def test_tweet_loader(self):
        delete_all_entities()
        path = guess_path("twitter-files")
        filename = "tweets.20150506-180056.rest-desmontandoaciudadanos.json"
        load_all_entities(path, filename)
        
        
        '''
        manager = sch.Manager()
        session = manager.get_session()

        tweet_loader(path, filename, session, regs_per_commit=100)
        hashtag_loader(path, filename, session, regs_per_commit=100)
        tweet_url_loader(path, filename, session, regs_per_commit=100)
        user_mention_loader(path, filename, session, regs_per_commit=100)
        user_url_loader(path, filename, session, regs_per_commit=100)
        '''
        pass


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.test_tweet_loader']
    unittest.main()