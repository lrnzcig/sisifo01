# -*- coding: utf-8 -*-
'''
Created on 28 de abr. de 2015

@author: lorenzorubio
'''
import unittest
import os
from nltk.twitter import Twitter
from nltk.twitter.util import guess_path

def get_search_tweets(keywords, date_limit=None, lang='es', limit=1000000, to_screen=False):
    tw = Twitter()
    tw.tweets(keywords=keywords, limit=limit, to_screen=to_screen, stream=False, lang=lang, 
              date_limit=date_limit, retries_after_twython_exception=3)


class Test(unittest.TestCase):


    def testGetSearchTweets(self):
        with open(os.path.join(guess_path("twitter-control"), "search-keywords.txt")) as f:
            keywords = [line.strip().replace('@', '') for line in f.readlines()]
        get_search_tweets(" OR ".join(keywords), date_limit=(2015, 5, 28, 17, 0))
        #get_search_tweets("#nopodeis", date_limit=(2015, 5, 29, 23, 40))
        pass


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testGetSearchTweets']
    unittest.main()