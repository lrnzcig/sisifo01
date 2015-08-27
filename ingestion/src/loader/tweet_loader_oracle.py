'''
Created on 29 de abr. de 2015

@author: lorenzorubio
'''
import unittest
import os
from nltk.twitter.util import guess_path
from loader.tweet_loader_abstract import TweetLoaderAbstract
from load_database.twitter_tables import manager as twitter_tables_manager
from load_database.external_tables import load_dir_manager, tweet_load, user_load
import connection.sisifo_connection as sisifo_connection



class TweetLoader(TweetLoaderAbstract):
    '''
    utility class, loads tweets from NLTK json file using SQLLoader (fast Oracle utility)
    '''    

    def __init__(self, oracle_path):
        self.conn = sisifo_connection.SisifoConnection()
        manager = twitter_tables_manager.Manager(self.conn)

        TweetLoaderAbstract.__init__(self, manager)
        
        self.oracle_path = oracle_path 
    
    def tweet_loader(self, path, filename, regs_per_commit=None):
        tweets = TweetLoaderAbstract._get_tweet_dfs(self, path, filename)
        
        '''
        TODO
        
        - careful with encoding and python 2/3
        
        - mixed types warning?
        
        - SO question?
        '''
        tfn = filename + ".tweet.csv"
        tweets.to_csv(os.path.join(self.oracle_path, tfn), header=True, sep=';')
        et = tweet_load.Tweet_load(tfn, self.conn)
        et.recreate_external_table()
        et.insert_into_target()
    
    def user_loader(self, path, filename, regs_per_commit=None):   
        users = TweetLoaderAbstract._get_user_dfs(self, path, filename)

        tfn = filename + ".user.csv"
        users.to_csv(os.path.join(self.oracle_path, tfn), header=True, sep=';')
        et = user_load.User_load(tfn, self.conn)
        et.recreate_external_table()
        et.insert_into_target()


    '''
    TODO
    
    - rest of loaders
    '''    
    def hashtag_loader(self, path, filename, regs_per_commit=None):
        hashtags = TweetLoaderAbstract._get_hashtag_dfs(self, path, filename)
    
    def tweet_url_loader(self, path, filename, regs_per_commit=None):
        tweeturls = TweetLoaderAbstract._get_tweet_url_dfs(self, path, filename)
            
    def user_mention_loader(self, path, filename, regs_per_commit=None):
        usermentions = TweetLoaderAbstract._get_user_mention_dfs(self, path, filename)
    
    def user_url_loader(self, path, filename, regs_per_commit=None):
        userurls = TweetLoaderAbstract._get_user_url_dfs(self, path, filename)
        
def cleanup_oracle_log_files(path):
    files = os.listdir(path)
    for f in files:
        if f.endswith('.log') | f.endswith('.bad'):
            os.remove(os.path.join(path, f))
                        
    
class Test(unittest.TestCase):

    def test_tweet_loader(self):
        oracle_path = os.path.abspath("/Volumes/extsisifo/OracleSQLLoader")
        dumper = TweetLoader(oracle_path)
        dumper.delete_all_entities()
        
        # cleanup log files in the directory
        cleanup_oracle_log_files(oracle_path)

        # recreate oracle's object for directory
        load_dir_m = load_dir_manager.Load_dir(oracle_path, dumper.conn)
        load_dir_m.drop()
        load_dir_m.create()

        path = guess_path("twitter-files")
        filename = "tweets.20150506-180056.rest-desmontandoaciudadanos.json"
        dumper.load_all_entities(path, filename)
        for f in os.listdir(path):
            if f.endswith('rest-municipales.json'):
                print("Loading file... " + f)
                dumper.load_all_entities(path, f)
        
        
        pass


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.test_tweet_loader']
    unittest.main()