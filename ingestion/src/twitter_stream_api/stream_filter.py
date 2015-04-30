'''
Created on 29 de abr. de 2015

@author: lorenzorubio
'''
import unittest
import os
from nltk.twitter import Twitter
from nltk.twitter.util import guess_path

def stream_filter(keywords):
    tw = Twitter()
    tw.tweets(keywords=keywords, limit=10000000, to_screen=False, stream=True, lang='es',
              date_limit=(2015, 5, 26, 0, 0, 0))
    

class Test(unittest.TestCase):


    def test_stream_filter(self):
        with open(os.path.join(guess_path("twitter-control"), "streaming-keywords.txt")) as f:
            keywords = [line.strip() for line in f.readlines()]
        stream_filter(keywords)
        pass


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.test_stream_filter']
    unittest.main()