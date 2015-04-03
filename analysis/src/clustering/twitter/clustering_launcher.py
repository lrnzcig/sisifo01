'''
Created on 3 de abr. de 2015

@author: lorenzorubio
'''
import unittest
from clustering.twitter import clustering_simple

class Test(unittest.TestCase):


    def testCluster(self):
        #clustering(['ahorapodemos', 'psoe', 'ppopular', 'ciudadanoscs', 'upyd'], number_of_clusters=6)
        clustering_simple.clustering(['ahorapodemos', 'psoe', 'ppopular', 'ciudadanoscs', 'upyd', 
                    'marianorajoy', 'albert_rivera', 'rosadiezupyd', 'sanchezcastejon'], number_of_clusters=6)
        '''
        clustering(['ahorapodemos', 'psoe', 'ppopular', 'ciudadanoscs', 'upyd', 
                    'marianorajoy', 'albert_rivera', 'rosadiezupyd', 'sanchezcastejon',
                    'alfonsomerlos', 'carloscuestaem'], number_of_clusters=6)
                    '''
        pass


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testCluster']
    unittest.main()