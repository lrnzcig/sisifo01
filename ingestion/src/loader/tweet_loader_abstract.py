'''
Created on 29 de abr. de 2015

@author: lorenzorubio
'''
import os
import re
import pandas as pd
from nltk.twitter.util import json2csv, json2csv_entities
import logging.handlers



class TweetLoaderAbstract():
    '''
    utility abstract class, base for loading tweets from NLTK json file
    '''
    tweet_column_list = ['created_at', 'favorite_count', 'in_reply_to_status_id', 'in_reply_to_user_id',
                         'retweet_count', 'text', 'truncated', 'user.id']
    user_column_list = ['created_at', 'contributors_enabled', 'description', 'favourites_count',
                        'followers_count', 'friends_count', 'is_translator', 'listed_count',
                        'location', 'name', 'protected', 'screen_name', 'statuses_count', 'url',
                        'verified', 'profile_link_color']    

    def __init__(self, manager):
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)
        fh = logging.handlers.RotatingFileHandler('/Users/lorenzorubio/Downloads/logtest.log', maxBytes=102400000, backupCount=5)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)
        
        self.manager = manager

        
    def load_all_entities(self, path, filename):
        self.tweet_loader(path, filename)
        self.user_loader(path, filename)
        #self.hashtag_loader(path, filename)
        #self.tweet_url_loader(path, filename)
        #self.user_mention_loader(path, filename)
        #self.user_url_loader(path, filename)
    
    def delete_all_entities(self):
        self.manager.delete_all()
    
    def tweet_loader(self, path, filename):
        raise NotImplemented()
    
    
    def _get_tweet_dfs(self, path, filename):
        with open(os.path.join(path, filename)) as f: 
            json2csv(f, 
                     os.path.join(path, 'temp.csv'),
                     ['id'] + self.tweet_column_list)
        
        tweets = pd.DataFrame.from_csv(os.path.join(path, 'temp.csv'), index_col=0, header=0, encoding="utf8")
        
        with open(os.path.join(path, filename)) as f: 
            json2csv_entities(f, 
                              os.path.join(path, 'temp2.csv'),
                              ['id'], 'retweeted_status', ['id'] + self.tweet_column_list)
        
        orig_tweets = pd.DataFrame.from_csv(os.path.join(path, 'temp2.csv'), index_col=1, header=0, encoding="utf8")
        retweet_info = pd.DataFrame.from_csv(os.path.join(path, 'temp2.csv'), index_col=0, header=0, encoding="utf8")
        
        return self._format_tweet_dfs(tweets, orig_tweets, retweet_info)


    def _format_tweet_dfs(self, tweets, orig_tweets, retweet_info):
        '''
        return just one list of tweets with no duplicates and information of retweets
        '''
        # concatenate dfs & column names
        orig_tweets.drop('id', axis=1, inplace=True)
        orig_tweets.index.name = 'id'
        orig_tweets.columns = self.tweet_column_list
        # retweet, retweeted_id & retweeted_user_id
        tweets['retweet'] = 0 
        tweets.loc[retweet_info.index, 'retweet'] = 1
        tweets['retweet_id'] = None
        tweets.loc[retweet_info.index, 'retweet_id'] = retweet_info['retweeted_status.id']
        tweets['retweet_user_id'] = None
        tweets.loc[retweet_info.index, 'retweet_user_id'] = retweet_info['retweeted_status.user.id']
        orig_tweets['retweet'] = 0 
        orig_tweets['retweet_id'] = None
        orig_tweets['retweet_user_id'] = None

        # concat        
        all_tweets = pd.concat([tweets, orig_tweets])
        all_tweets.drop_duplicates(inplace=True)
             
        
        # problems with carriage returns
        self._cleanup_carriage_returns(all_tweets, 'text', 'text_clean')
        
        return all_tweets
    
    def _cleanup_carriage_returns(self, df, old_column_name, new_column_name):
        df[new_column_name] = df.apply(lambda row: re.sub('\n', ' ', row[old_column_name]), axis=1)
        df.drop(old_column_name, axis=1, inplace=True)
        return df

    def user_loader(self, path, filename):   
        raise NotImplemented()
    
    def _get_user_dfs(self, path, filename):
        with open(os.path.join(path, filename)) as f: 
            json2csv(f, 
                     os.path.join(path, 'temp.csv'),
                     ['user.id'] + ['user.' + x for x in self.user_column_list])
        # no index since the same user might be two times in the file
        users = pd.DataFrame.from_csv(os.path.join(path, 'temp.csv'), index_col=0, header=0, encoding="utf8")
        users.columns = self.user_column_list
        
        # users from retweets
        with open(os.path.join(path, filename)) as f: 
            json2csv_entities(f, 
                              os.path.join(path, 'temp2.csv'),
                              ['id'], 'retweeted_status', ['user.id'] + ['user.' + x for x in self.user_column_list])
        
        orig_tweets = pd.DataFrame.from_csv(os.path.join(path, 'temp2.csv'), index_col=1, header=0, encoding="utf8")
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
        tot_users['description'].fillna(' ', inplace=True) # TODO last one in csv file makes it fail if empty
        tot_users['location'].fillna('', inplace=True)
        tot_users['name'].fillna('', inplace=True)
        tot_users['url'].fillna('', inplace=True)
        
        # problems with carriage returns
        self._cleanup_carriage_returns(tot_users, 'name', 'name_clean')
        self._cleanup_carriage_returns(tot_users, 'location', 'location_clean')
        self._cleanup_carriage_returns(tot_users, 'description', 'description_clean')
        
        return tot_users

    def hashtag_loader(self, path, filename):   
        raise NotImplemented()
    
    
    def _get_hashtag_dfs(self, path, filename):
        json2csv_entities(os.path.join(path, filename), 
                          os.path.join(path, 'temp.csv'),
                          ['id'], 'hashtags', ['text'])
        hashtags = pd.DataFrame.from_csv(os.path.join(path, 'temp.csv'), index_col=None, header=0, encoding="utf8")
        # remove duplicates
        hashtags.drop_duplicates(inplace=True)

        # TODO hashtags from retweets 
        return hashtags
            
    def tweet_url_loader(self, path, filename):   
        raise NotImplemented()
    
    def _get_tweet_url_dfs(self, path, filename):
        json2csv_entities(os.path.join(path, filename), 
                          os.path.join(path, 'temp.csv'),
                          ['id'], 'urls', ['url'])
        tweeturls = pd.DataFrame.from_csv(os.path.join(path, 'temp.csv'), index_col=None, header=0, encoding="utf8")
        # remove duplicates
        tweeturls.drop_duplicates(inplace=True)

        return tweeturls
            
    def user_mention_loader(self, path, filename):   
        raise NotImplemented()
    
    def _get_user_mention_dfs(self, path, filename):
        json2csv_entities(os.path.join(path, filename), 
                          os.path.join(path, 'temp.csv'),
                          ['id'], 'user_mentions', ['id'])
        usermentions = pd.DataFrame.from_csv(os.path.join(path, 'temp.csv'), index_col=None, header=0, encoding="utf8")
        # remove duplicates
        usermentions.drop_duplicates(inplace=True)
        return usermentions

    
    def user_url_loader(self, path, filename):   
        raise NotImplemented()
    
    def _get_user_url_dfs(self, path, filename):
        temp_file = os.path.join(path, 'temp.csv')
        json2csv_entities(os.path.join(path, filename), 
                          temp_file,
                          ['id'], {'user' : 'urls'}, ['url'])
        if os.stat(temp_file).st_size == 0:
            # there are no user urls, not at all uncommon
            return None
        userurls = pd.DataFrame.from_csv(temp_file, index_col=None, header=0, encoding="utf8")
        # remove duplicates
        userurls.drop_duplicates(inplace=True)
        return userurls                        