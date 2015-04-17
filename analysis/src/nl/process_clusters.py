'''
Created on 14 de abr. de 2015

@author: lorenzorubio
'''
import unittest
import pandas as pd
from schema_aux import list_of_user_clustering as luc
from schema_aux import twitter_schema as sch

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
    
    return
    

class Test(unittest.TestCase):


    def testProcess(self):
        process_clusters()
        pass


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testProcess']
    unittest.main()