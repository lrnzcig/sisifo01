'''
Created on 29 de abr. de 2015

@author: lorenzorubio
'''
import unittest
from datetime import datetime
import math
from nltk.compat import UTC
from nltk.twitter.util import guess_path
from schema_aux import twitter_schema as sch
from loader.tweet_loader_abstract import TweetLoaderAbstract
# performance checks
from pympler import summary, muppy
import psutil



class TweetLoader(TweetLoaderAbstract):
    '''
    utility class, loads tweets from NLTK json file using SQLAlchemy
    '''    
    default_regs_per_commit = 10000
    
    # cache keys
    tweet_key = sch.Tweet.__tablename__
    user_key = sch.User.__tablename__
    hashtag_key = sch.Hashtag.__tablename__
    tweet_url_key = sch.TweetUrl.__tablename__
    user_mention_key = sch.UserMention.__tablename__
    user_url_key = sch.UserMention.__tablename__
    

    def __init__(self, user=None):
        manager = sch.Manager(alchemy_echo=False, user=user)
        TweetLoaderAbstract.__init__(self, manager)
        
        self.session = self.manager.get_session()

        self.init_cache()
        self.show_memory_usage = False
        
    def init_cache(self):
        self.cache = {self.tweet_key : {}, self.user_key : {},
                      self.hashtag_key : {}, self.tweet_url_key : {},
                      self.user_mention_key : {}, self.user_url_key : {} }

    def load_all_entities(self, path, filename):
        TweetLoaderAbstract.load_all_entities(self, path, filename)
        self.session.close()
    
    
    def delete_all_entities(self):
        TweetLoaderAbstract.delete_all_entities(self)
        self.session = self.manager.get_session()        
    
    '''
    TODO 
    
    this belongs to abstract, even if it does not apply to Oracle?
    '''
    def boolean2char(self, boolean_value):
        if boolean_value:
            return 1
        return 0
    
    def tweet_loader(self, path, filename, regs_per_commit=default_regs_per_commit):
        tweets = TweetLoaderAbstract._get_tweet_dfs(self, path, filename, do_clean_duplicates=True, do_cleanup_carriage_returns=False)
        
        tweets['truncated'] = tweets.apply(lambda row: self.boolean2char(row['truncated']), axis = 1)
        
        cache = self.cache[self.tweet_key]
        counter = 0
        for tweet in tweets.iterrows():
            created_at = datetime.strptime(tweet[1]['created_at'], '%a %b %d %H:%M:%S +0000 %Y').replace(tzinfo=UTC)
            tweet_id = int(tweet[0])
            old_tweet = sch.Tweet.get(self.session, cache, id=tweet_id)
            if old_tweet:
                if old_tweet.retweet_count < tweet[1]['retweet_count'] or old_tweet.favorite_count < tweet[1]['favorite_count']:
                    # it is more recent
                    old_tweet.retweet_count = tweet[1]['retweet_count']
                    old_tweet.favorite_count = tweet[1]['favorite_count']
                continue
            # problems with numerics
            if math.isnan(tweet[1]['in_reply_to_user_id']):
                in_reply_to_user_id = None
            else:
                in_reply_to_user_id = int(tweet[1]['in_reply_to_user_id'])
            if math.isnan(tweet[1]['in_reply_to_status_id']):
                in_reply_to_status_id = None
            else:
                in_reply_to_status_id = int(tweet[1]['in_reply_to_status_id'])
            if isinstance(tweet[1]['retweeted_status.id'], str):
                retweeted_id = int(tweet[1]['retweeted_status.id'])
            else:
                retweeted_id = None
            if isinstance(tweet[1]['retweeted_status.user.id'], str):
                retweeted_user_id = int(tweet[1]['retweeted_status.user.id'])
            else:
                retweeted_user_id = None
            user_id = int(tweet[1]['user.id'])
            sch.Tweet.as_cached(self.session, cache, created_at=created_at, favorite_count=tweet[1]['favorite_count'],
                                id=tweet_id, in_reply_to_status_id=in_reply_to_status_id,
                                retweet_count=tweet[1]['retweet_count'], in_reply_to_user_id=in_reply_to_user_id,
                                retweet=tweet[1]['retweet'], text=tweet[1]['text'], truncated=tweet[1]['truncated'],
                                user_id=user_id, retweeted_id=retweeted_id, retweeted_user_id=retweeted_user_id)
            counter += 1
            self._commit(counter, regs_per_commit)
        self._commit()
    
    
    def user_loader(self, path, filename, regs_per_commit=default_regs_per_commit):
        tot_users = TweetLoaderAbstract._get_user_dfs(self, path, filename, do_clean_duplicates=True, do_cleanup_carriage_returns=False)

        tot_users['is_translator'] = tot_users.apply(lambda row: self.boolean2char(row['is_translator']), axis = 1) 
        tot_users['protected'] = tot_users.apply(lambda row: self.boolean2char(row['protected']), axis = 1) 
        tot_users['verified'] = tot_users.apply(lambda row: self.boolean2char(row['verified']), axis = 1) 
        
        cache = self.cache[self.user_key] 
        counter = 0
        for user in tot_users.iterrows():
            user_id = int(user[0])
            old_user = sch.User.get(self.session, cache, id=user_id)
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
                    old_user.profile_link_color = user[1]['profile_link_color']
                continue
            created_at = datetime.strptime(user[1]['created_at'], '%a %b %d %H:%M:%S +0000 %Y').replace(tzinfo=UTC)
            sch.User.as_cached(self.session, cache, id=user_id, screen_name=user[1]['screen_name'], created_at=created_at,
                               contributors_enabled=user[1]['contributors_enabled'], description=user[1]['description'],
                               favourites_count = user[1]['favourites_count'], followers_count = user[1]['followers_count'],
                               friends_count = user[1]['friends_count'], is_translator = user[1]['is_translator'],
                               listed_count = user[1]['listed_count'], location = user[1]['location'], name = user[1]['name'],
                               protected = user[1]['protected'], statuses_count = user[1]['statuses_count'], url=user[1]['url'],
                               verified = user[1]['verified'], profile_link_color = user[1]['profile_link_color'])
            counter += 1
            self._commit(counter, regs_per_commit)
        self._commit()

    
    def hashtag_loader(self, path, filename, regs_per_commit=default_regs_per_commit):
        hashtags = TweetLoaderAbstract._get_hashtag_dfs(self, path, filename)
        
        cache = self.cache[self.hashtag_key]

        counter = 0
        for hashtag in hashtags.iterrows():
            hashtag = sch.Hashtag.as_unique(self.session, cache, tweet_id=hashtag[1][0], hashtag=hashtag[1][1])
            counter += 1
            if regs_per_commit and counter % regs_per_commit == 0:
                self._commit(counter)
        self._commit()
    
    def tweet_url_loader(self, path, filename, regs_per_commit=default_regs_per_commit):
        tweeturls = TweetLoaderAbstract._get_tweet_url_dfs(self, path, filename)

        cache = self.cache[self.tweet_url_key]

        counter = 0
        for tweeturl in tweeturls.iterrows():
            tweeturl = sch.TweetUrl.as_unique(self.session, cache, tweet_id=tweeturl[1][0], url=tweeturl[1][1])
            counter += 1
            self._commit(counter)
        self._commit()
            
    def user_mention_loader(self, path, filename, regs_per_commit=default_regs_per_commit):
        usermentions = TweetLoaderAbstract._get_user_mention_dfs(self, path, filename)

        cache = self.cache[self.user_mention_key]
        tweet_cache = self.cache[self.tweet_key]

        counter = 0
        for usermention in usermentions.iterrows():
            tweet_id = usermention[1][0]
            old_tweet = sch.Tweet.get(self.session, tweet_cache, id=tweet_id)
            if old_tweet == None:
                raise RuntimeError("Incongruent data - there's user mentions for a tweet that does not exist. Tweet id: " + str(tweet_id))
            usermention = sch.UserMention.as_unique(self.session, cache, tweet_id=tweet_id, source_user_id=old_tweet.user_id, 
                                          target_user_id=usermention[1][1])
            counter += 1
            self._commit(counter)
        self._commit()
    
    def user_url_loader(self, path, filename, regs_per_commit=default_regs_per_commit):
        userurls = TweetLoaderAbstract._get_user_url_dfs(self, path, filename)
        if userurls == None:
            # there are no user urls, not at all uncommon
            return

        cache = self.cache[self.user_url_key]

        counter = 0
        for userurl in userurls.iterrows():
            userurl = sch.TweetUrl.as_unique(self.session, cache, tweet_id=userurl[1][0], url=userurl[1][1])
            counter += 1
            self._commit(counter)
        self._commit()
        
    def _commit(self, counter=0, regs_per_commit=default_regs_per_commit):
        regs = self.default_regs_per_commit
        if regs_per_commit:
            regs = regs_per_commit
        if regs and counter % regs == 0:
            self.session.commit()
            self.session.expunge_all()
            self.session.close()
            self.session = self.manager.get_session()
            if self.show_memory_usage == True and counter % 15000 == 0:
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
        #dumper = TweetLoader()
        dumper = TweetLoader(user='TWEETDESMONTANDO')

        '''
        TODO
        
        - cleanup datatypes in abstract
        '''
        dumper.delete_all_entities()
        path = guess_path("twitter-files")
        filename = "tweets.20150506-180056.rest-desmontandoaciudadanos.json"
        dumper.load_all_entities(path, filename)
        
        #dumper.user_loader(path, filename)#, regs_per_commit=1)
        pass


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.test_tweet_loader']
    unittest.main()