'''
Created on 19/3/2015

@author: lorenzorubio
'''
import unittest
from database import sisifo_connection


class Test(unittest.TestCase):


    def testConnect(self):
        conn = sisifo_connection.SisifoConnection().get()
        print(conn.version)
        self.assertEqual(conn.version, "11.2.0.2.0", "Error connecting to oracle")
        conn.close()
        pass


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testConnect']
    unittest.main()