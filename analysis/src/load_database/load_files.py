'''
Created on 30/3/2015

@author: lorenzorubio
'''
import unittest
import os
from database import sisifo_connection
from load_database.definitions import type_of_file_enum
from load_database.external_tables import (tweet_load, user_load, user_mention_load, user_friend_load)
from load_database.twitter_tables import manager as twitter_tables_manager

# http://code.activestate.com/recipes/410692/
# move to separate class!!
class switch(object):
    def __init__(self, value):
        self.value = value
        self.fall = False

    def __iter__(self):
        """Return the match method once, then stop"""
        yield self.match
        raise StopIteration
    
    def match(self, *args):
        """Indicate whether or not to enter a case suite"""
        if self.fall or not args:
            return True
        elif self.value in args: # changed for v1.5, see below
            self.fall = True
            return True
        else:
            return False
# end move!

def detect_type_of_file(filename, files):
    for f in files:
        if (f == filename + ".cr"):
            # there is a file in the same directory correcting carriage return
            # do not load this file
            return 0, False
    
    if filename.startswith("tweets_"):
        return type_of_file_enum.tweets, True
    elif filename.startswith("tweetUsers_"):
        return type_of_file_enum.users, True
    elif filename.startswith("friends_"):
        return type_of_file_enum.friends, True
    elif filename.startswith("tweetHashtags_"):
        return type_of_file_enum.hashtags, True
    elif filename.startswith("tweetUrls_"):
        return type_of_file_enum.tweet_url, True
    elif filename.startswith("tweetUserMentions_"):
        return type_of_file_enum.user_mentions, True
    elif filename.startswith("tweetUserUrls_"):
        return type_of_file_enum.user_urls, True
    elif filename.startswith("favorites_"):
        return type_of_file_enum.favorites, True
    return 0, False
    
def load_files(path, conn):
    files = os.listdir(path)
    for f in files:
        type_of_file, load = detect_type_of_file(f, files)
        if (load == False):
            print(f + " not loaded")
            continue
        print(f + " is type " + str(type_of_file))
        for case in switch(type_of_file):
            doLoad = False
            if (case(type_of_file_enum.tweets)):
                et = tweet_load.Tweet_load(f, conn)
                doLoad = True
            elif (case(type_of_file_enum.users)):
                et = user_load.User_load(f, conn, fast=False)
                doLoad = True
            elif (case(type_of_file_enum.user_mentions)):
                et = user_mention_load.User_mention_load(f, conn)
                doLoad = True
            elif (case(type_of_file_enum.friends)):
                et = user_friend_load.User_friend_load(f, conn)
                doLoad = True
            if (doLoad == True):
                et.recreate_external_table()
                et.insert_into_target()
    return

def cleanup_oracle_log_files(path):
    files = os.listdir(path)
    for f in files:
        if f.endswith('.log') | f.endswith('.bad'):
            os.remove(path + f)
    

class Test(unittest.TestCase):


    def testLoadFiles(self):
        conn = sisifo_connection.SisifoConnection()
        manager = twitter_tables_manager.Manager(conn)
        manager.cleanup_tables()
        path = '/Users/lorenzorubio/Documents/datascience/sisifo01/restapi1/'
        cleanup_oracle_log_files(path)
        load_files(path, conn)
        conn.close()
        pass


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testLoadFiles']
    unittest.main()