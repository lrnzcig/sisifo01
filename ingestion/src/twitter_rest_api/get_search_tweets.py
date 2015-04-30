'''
Created on 28 de abr. de 2015

@author: lorenzorubio
'''
import unittest
from nltk.twitter import Twitter

def get_search_tweets(keywords):
    tw = Twitter()
    tw.tweets(keywords=keywords, limit=100000, to_screen=False, stream=False, lang='es', 
              date_limit=(2015, 4, 24, 0, 0))

class Test(unittest.TestCase):


    def testGetSearchTweets(self):
        get_search_tweets("@ahorapodemos")
        pass


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testGetSearchTweets']
    unittest.main()