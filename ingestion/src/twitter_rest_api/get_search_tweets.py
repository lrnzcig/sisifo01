# -*- coding: utf-8 -*-
'''
Created on 28 de abr. de 2015

@author: lorenzorubio
'''
import unittest
import os
from nltk.twitter import TweetWriter, Query, credsfromfile
from nltk.twitter.util import guess_path
from twython.exceptions import TwythonError

def get_search_tweets(keywords, date_limit=None, lang='es', limit=10000000, to_screen=False):
    #tw = Twitter()
    #tw.tweets(keywords=keywords, limit=limit, to_screen=to_screen, stream=False, lang=None, 
    #          date_limit=date_limit)
    handler = TweetWriter(limit=limit, upper_date_limit=None,
                          lower_date_limit=date_limit, repeat=False,
                          gzip_compress=False)
    _oauth = credsfromfile()
    query = Query(**_oauth)
    query.register(handler)
    
    retries = 0
    retries_after_twython_exception = 10
    print("Retries left: {0}".format(retries_after_twython_exception - retries))
    while True:
        max_id = handler.max_id
        try:
            tweets = query.search_tweets(keywords=keywords, limit=limit, lang=None,
                                         max_id=max_id)
            for tweet in tweets:
                handler.handle(tweet)
        except TwythonError as e:
            print("=> Fatal error in Twython request -{0}".format(e))
            if retries_after_twython_exception == retries:
                print("No more trials left, raising exception")
                raise e
            retries += 1
            print("Continue fetching tweets. Retries left: {0}".format(retries_after_twython_exception - retries))
            continue
        if not (handler.do_continue() and handler.repeat):
            break
    handler.on_finish()



class Test(unittest.TestCase):


    def testGetSearchTweets(self):
        with open(os.path.join(guess_path("twitter-control"), "search-keywords-refugee.txt")) as f:
            keywords = [line.strip().replace('@', '') for line in f.readlines()]
        print(" OR ".join(keywords))
        get_search_tweets(" OR ".join(keywords), date_limit=(2015, 8, 30, 0, 0))
        #get_search_tweets("#nopodeis", date_limit=(2015, 5, 29, 23, 40))
        pass


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testGetSearchTweets']
    unittest.main()