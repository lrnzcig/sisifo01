'''
Created on 29 de abr. de 2015

@author: lorenzorubio
'''
import unittest
import os
from nltk.twitter import Twitter
from nltk.twitter.util import guess_path

def stream_filter(keywords, date_limit=None, lang='es', limit=1000000, to_screen=False):
    tw = Twitter()
    tw.tweets(keywords=keywords, limit=limit, to_screen=to_screen, stream=True, lang=lang,
              date_limit=date_limit)
    

class Test(unittest.TestCase):


    def test_stream_filter(self):
        with open(os.path.join(guess_path("twitter-control"), "streaming-keywords.txt")) as f:
            keywords = [line.strip() for line in f.readlines()]
        stream_filter(keywords, date_limit=(2015, 5, 7, 13, 0, 0))
        pass


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.test_stream_filter']
    unittest.main()