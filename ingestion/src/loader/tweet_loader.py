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
import logging.handlers
# performance checks
from pympler import summary, muppy
import psutil



class TweetLoader():
    '''
    utility class, loads tweets from NLTK json file
    '''
    user_column_list = ['created_at', 'contributors_enabled', 'description', 'favourites_count',
                        'followers_count', 'friends_count', 'is_translator', 'listed_count',
                        'location', 'name', 'protected', 'screen_name', 'statuses_count', 'url',
                        'verified']
    
    default_regs_per_commit = 10000

    def __init__(self):
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)
        fh = logging.handlers.RotatingFileHandler('/Users/lorenzorubio/Downloads/logtest.log', maxBytes=102400000, backupCount=5)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)

        # one session for the full load to make use of the cache
        self.manager = sch.Manager(alchemy_echo=False)
        self.session = self.manager.get_session()

    def load_all_entities(self, path, filename):
        self.tweet_loader(path, filename, regs_per_commit=self.default_regs_per_commit)
        self.user_loader(path, filename, regs_per_commit=self.default_regs_per_commit)
        self.hashtag_loader(path, filename, regs_per_commit=self.default_regs_per_commit)
        self.tweet_url_loader(path, filename, regs_per_commit=self.default_regs_per_commit)
        self.user_mention_loader(path, filename, regs_per_commit=self.default_regs_per_commit)
        self.user_url_loader(path, filename, regs_per_commit=self.default_regs_per_commit)
        self.session.close()
    
    
    def delete_all_entities(self):
        self.manager.delete_all()
        self._commit()
        self.session = self.manager.get_session()        
    
    
    def tweet_loader(self, path, filename, regs_per_commit=None):
        json2csv(os.path.join(path, filename), 
                 os.path.join(path, 'temp.csv'),
                 ['created_at', 'favorite_count', 'id', 'in_reply_to_status_id', 'in_reply_to_user_id', 'retweet_count',
                  'text', 'truncated', {'user' : {'id'}}])
        
        tweets = pd.DataFrame.from_csv(os.path.join(path, 'temp.csv'), index_col=2, header=None, encoding="utf8")
        
        json2csv_entities(os.path.join(path, filename), 
                          os.path.join(path, 'temp2.csv'),
                          ['id'], 'retweeted_status', ['created_at', 'favorite_count', 'id', 'in_reply_to_status_id', 'in_reply_to_user_id', 'retweet_count',
                                                       'text', 'truncated', {'user' : {'id'}}])
        
        orig_tweets = pd.DataFrame.from_csv(os.path.join(path, 'temp2.csv'), index_col=0, header=None, encoding="utf8")
        
        counter = 0
        for tweet in tweets.iterrows():
            created_at = datetime.strptime(tweet[1][0], '%a %b %d %H:%M:%S +0000 %Y').replace(tzinfo=UTC)
            tweet_id = tweet[0]
            old_tweet = sch.Tweet.get(self.session, id=tweet_id)
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
                original_tweet = sch.Tweet.get(self.session, id=original_tweet_id)
                if not original_tweet:
                    # insert it already
                    orig_created_at = datetime.strptime(original_tweet_df[1], '%a %b %d %H:%M:%S +0000 %Y').replace(tzinfo=UTC)
                    truncated = False
                    if original_tweet_df[8]:
                        # the original is numpy.bool_ and SQLAlchemy does not accept it
                        truncated = True
                    sch.Tweet.as_cached(self.session, created_at=orig_created_at, favorite_count=original_tweet_df[2],
                                        id=original_tweet_id, in_reply_to_status_id=original_tweet_df[4],
                                        in_reply_to_user_id=original_tweet_df[5], retweet_count=original_tweet_df[6],
                                        retweet=False, text=original_tweet_df[7], truncated=truncated,
                                        user_id=original_tweet_df[9], retweeted_id=None, retweeted_user_id=None)
                retweeted_id = original_tweet_id
                retweeted_user_id = original_tweet_df[9]
            sch.Tweet.as_cached(self.session, created_at=created_at, favorite_count=tweet[1][1],
                                id=tweet_id, in_reply_to_status_id=tweet[1][3],
                                in_reply_to_user_id=tweet[1][4], retweet_count=tweet[1][5],
                                retweet=retweet, text=tweet[1][6], truncated=tweet[1][7],
                                user_id=tweet[1][8], retweeted_id=retweeted_id, retweeted_user_id=retweeted_user_id)
            counter += 1
            self._commit(counter)
        self._commit()
    
    
    def user_loader(self, path, filename, regs_per_commit=None):   
        json2csv(os.path.join(path, filename), 
                 os.path.join(path, 'temp.csv'),
                 [{'user' : ['id'] + self.user_column_list}])
        # no index since the same user might be two times in the file
        users = pd.DataFrame.from_csv(os.path.join(path, 'temp.csv'), index_col=0, header=None, encoding="utf8")
        users.columns = self.user_column_list
        
        # users from retweets
        json2csv_entities(os.path.join(path, filename), 
                          os.path.join(path, 'temp2.csv'),
                          ['id'], 'retweeted_status', [{'user' : ['id'] + self.user_column_list}])
        
        orig_tweets = pd.DataFrame.from_csv(os.path.join(path, 'temp2.csv'), index_col=1, header=None, encoding="utf8")
        orig_tweets.columns = ['tweet_id'] + self.user_column_list
        
        # drop duplicates by and concat into tot_users
        users['index'] = users.index
        users.drop_duplicates(subset=['index'], take_last = True, inplace=True)
        orig_tweets['index'] = orig_tweets.index
        orig_tweets.drop_duplicates(subset=['index'], take_last = True, inplace=True)
        orig_tweets.drop('tweet_id', axis=1, inplace=True)
        tot_users = pd.concat([orig_tweets, users])
        tot_users.drop_duplicates(subset=['index'], take_last = True, inplace=True)
        tot_users.drop('index', axis=1, inplace=True)
        
        
        # pandas reads as nan even for string columns => fill them with empty string
        tot_users['description'].fillna('', inplace=True)
        tot_users['location'].fillna('', inplace=True)
        tot_users['name'].fillna('', inplace=True)
        tot_users['url'].fillna('', inplace=True)
        counter = 0
        for user in tot_users.iterrows():
            user_id = user[0]
            old_user = sch.User.get(self.session, id=user_id)
            if old_user:
                if old_user.statuses_count < user[1]['statuses_count']:
                    # assumed: if the user twitted more, there could be modifications
                    old_user.contributors_enabled = user[1]['contributors_enabled']
                    old_user.description = user[1]['description']
                    old_user.favourites_count = user[1]['favourites_count']
                    old_user.followers_count = user[1]['followers_count']
                    old_user.friends_count = user[1]['friends_count']
                    old_user.is_translator = user[1]['is_translator']
                    old_user.listed_count = user[1]['listed_count']
                    old_user.location = user[1]['location']
                    old_user.name = user[1]['name']
                    old_user.protected = user[1]['protected']
                    old_user.statuses_count = user[1]['statuses_count']
                    old_user.url = user[1]['url']
                    old_user.verified = user[1]['verified']
                continue
            created_at = datetime.strptime(user[1]['created_at'], '%a %b %d %H:%M:%S +0000 %Y').replace(tzinfo=UTC)
            user = sch.User.as_cached(self.session, id=user_id, screen_name=user[1]['screen_name'], created_at=created_at,
                            contributors_enabled=user[1]['contributors_enabled'], description=user[1]['description'],
                            favourites_count = user[1]['favourites_count'], followers_count = user[1]['followers_count'],
                            friends_count = user[1]['friends_count'], is_translator = user[1]['is_translator'],
                            listed_count = user[1]['listed_count'], location = user[1]['location'], name = user[1]['name'],
                            protected = user[1]['protected'], statuses_count = user[1]['statuses_count'], url=user[1]['url'],
                            verified = user[1]['verified'])
            counter += 1
            self._commit(counter)
        self._commit()

    
    def hashtag_loader(self, path, filename, regs_per_commit=None):
        json2csv_entities(os.path.join(path, filename), 
                          os.path.join(path, 'temp.csv'),
                          ['id'], 'hashtags', ['text'])
        hashtags = pd.DataFrame.from_csv(os.path.join(path, 'temp.csv'), index_col=None, header=None, encoding="utf8")
        # TODO hashtags from retweets 
        
        # remove duplicates
        hashtags.drop_duplicates(inplace=True)
        counter = 0
        for hashtag in hashtags.iterrows():
            hashtag = sch.Hashtag.as_unique(self.session, tweet_id=hashtag[1][0], hashtag=hashtag[1][1])
            counter += 1
            if regs_per_commit and counter % regs_per_commit == 0:
                self._commit(counter)
        self._commit()
    
    def tweet_url_loader(self, path, filename, regs_per_commit=None):
        json2csv_entities(os.path.join(path, filename), 
                          os.path.join(path, 'temp.csv'),
                          ['id'], 'urls', ['url'])
        tweeturls = pd.DataFrame.from_csv(os.path.join(path, 'temp.csv'), index_col=None, header=None, encoding="utf8")

        # remove duplicates
        tweeturls.drop_duplicates(inplace=True)
        counter = 0
        for tweeturl in tweeturls.iterrows():
            tweeturl = sch.TweetUrl.as_unique(self.session, tweet_id=tweeturl[1][0], url=tweeturl[1][1])
            counter += 1
            self._commit(counter)
        self._commit()
            
    def user_mention_loader(self, path, filename, regs_per_commit=None):
        json2csv_entities(os.path.join(path, filename), 
                          os.path.join(path, 'temp.csv'),
                          ['id'], 'user_mentions', ['id'])
        usermentions = pd.DataFrame.from_csv(os.path.join(path, 'temp.csv'), index_col=None, header=None, encoding="utf8")

        # remove duplicates
        usermentions.drop_duplicates(inplace=True)
        counter = 0
        for usermention in usermentions.iterrows():
            tweet_id = usermention[1][0]
            old_tweet = sch.Tweet.get(self.session, id=tweet_id)
            if old_tweet == None:
                raise RuntimeError("Incongruent data - there's user mentions for a tweet that does not exist. Tweet id: " + str(tweet_id))
            usermention = sch.UserMention.as_unique(self.session, tweet_id=tweet_id, source_user_id=old_tweet.user_id, 
                                          target_user_id=usermention[1][1])
            counter += 1
            self._commit(counter)
        self._commit()
    
    def user_url_loader(self, path, filename, regs_per_commit=None):
        temp_file = os.path.join(path, 'temp.csv')
        json2csv_entities(os.path.join(path, filename), 
                          temp_file,
                          ['id'], {'user' : 'urls'}, ['url'])
        if os.stat(temp_file).st_size == 0:
            # there are no user urls, not at all uncommon
            return
        userurls = pd.DataFrame.from_csv(temp_file, index_col=None, header=None, encoding="utf8")

        # remove duplicates
        userurls.drop_duplicates(inplace=True)

        counter = 0
        for userurl in userurls.iterrows():
            userurl = sch.TweetUrl.as_unique(self.session, tweet_id=userurl[1][0], url=userurl[1][1])
            counter += 1
            self._commit(counter)
        self._commit()
        
    def _commit(self, counter=0, regs_per_commit=None):
        regs = self.default_regs_per_commit
        if regs_per_commit:
            regs = regs_per_commit
        if regs and counter % regs == 0:
            self.session.commit()
            self.session.expunge_all()
            # get the cache and pass it to next session - not good results
            #cache = getattr(self.session, '_u_cache', None)
            self.session.close()
            self.session = self.manager.get_session()
            #self.session._u_cache = cache
            if counter % 15000 == 0:
                self.memory_usage("counter = " + str(counter))
        
    def get_virtual_memory_usage_kb(self):
        """
        The process's current virtual memory size in Kb, as a float.

        """
        return float(psutil.Process().memory_info_ex().vms) / 1024.0

    def memory_usage(self, where):
        """
        Print out a basic summary of memory usage.
    
        """
        mem_summary = summary.summarize(muppy.get_objects())
        self.logger.info("Memory summary:" + where)
        for line in summary.format_(mem_summary, limit=2, sort='size', order='descending'):
            self.logger.info(line)
        self.logger.info("VM: %.2fMb" % (self.get_virtual_memory_usage_kb() / 1024.0))
                        
    
class Test(unittest.TestCase):

    def test_tweet_loader(self):
        dumper = TweetLoader()
        dumper.delete_all_entities()
        path = guess_path("twitter-files")
        filename = "tweets.20150506-180056.rest-desmontandoaciudadanos.json"
        dumper.load_all_entities(path, filename)
        
        
        '''
        dumper.user_loader(path, filename, regs_per_commit=10)
        '''
        pass


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.test_tweet_loader']
    unittest.main()