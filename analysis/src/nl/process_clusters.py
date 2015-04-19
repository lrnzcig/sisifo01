'''
Created on 14 de abr. de 2015

@author: lorenzorubio
'''
from __future__ import division
import unittest
import pandas as pd
from schema_aux import list_of_user_clustering as luc
from schema_aux import twitter_schema as sch
from nltk.tokenize import word_tokenize
from nltk.probability import FreqDist

def process_clusters():
    # example: get list of users 
    users_list_mgr = luc.Manager()
    users = users_list_mgr.get()
    pure_set_0 = users.loc[users['cluster_label'] == '0'].loc[users['additional_label'] == 'pure_set']
    print(len(pure_set_0))
    
    # example: get tweets of that pure_set
    schema_mgr = sch.Manager()
    session = schema_mgr.get_session()
    q = session.query(sch.Tweet).join(sch.User, sch.User.id == sch.Tweet.user_id) \
        .join(luc.ListOfUserClustering, luc.ListOfUserClustering.id == sch.User.id) \
        .filter(luc.ListOfUserClustering.cluster_label == '0').filter(luc.ListOfUserClustering.additional_label == 'pure_set')
    tweets_set_0 = pd.read_sql(q.statement, session.bind)
    print(tweets_set_0.shape)
    
    
    tweet1_tokens = word_tokenize(tweets_set_0.loc[1, 'text'])
    print(sorted(set(tweet1_tokens)))
    
    corpus = [word for tweet in tweets_set_0.loc[:, 'text'] for word in word_tokenize(tweet)]
    print(sorted(set(corpus)))
    print(len(set(corpus)))
    
    # diversity (how many times a word is repeated on average)
    print("diversity", len(corpus) / len(set(corpus)))
    
    # frequency distribution
    fdist = FreqDist(corpus)
    fdist.plot(50, cumulative=True)
    
    print(fdist['ahorapodemos'])
    return
    

class Test(unittest.TestCase):


    def testProcess(self):
        process_clusters()
        pass


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testProcess']
    unittest.main()