'''
Created on 19/3/2015

@author: lorenzorubio
'''
import unittest
from database import sisifo_connection
import pandas as pd

class Test(unittest.TestCase):


    def testQueryToDataframe(self):
        conn = sisifo_connection.SisifoConnection().get()
        df = pd.read_sql("select * from tweet", conn, index_col='ID')
        print(df)
        pass


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testQueryToDataframe']
    unittest.main()