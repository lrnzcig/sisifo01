'''
Created on 20/3/2015

@from: http://blog.dataquest.io/blog/plotting-senators/
'''
import unittest
import pandas as pd
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
from sklearn.decomposition import PCA

class Test(unittest.TestCase):


    def testCluster(self):
        '''
        Step 1.
        We have a csv file that contains all the votes from the 114th Senate.
        Each row contains the votes of an individual senator. Votes are coded as 0 for "No", 1 for "Yes", and 0.5 for "Abstain".
        
        ===> maybe 2 for abstain in my case
        '''
        votes = pd.read_csv("/Users/lorenzorubio/git/political-positions/senate.csv")
        # As you can see, there are 100 senators, and they voted on 15 bills (we subtract 3 because the first 3 columns aren't bills).
        print("==> votes.shape")
        print(votes.shape)
        print(votes)

        # We have more "Yes" votes than "No" votes overall
        print("==> value_counts")
        print(pd.value_counts(votes.iloc[:,3:].values.ravel()))
        
        '''
        Step 2.
        Initial clustering
        k-means clustering will try to make clusters out of the senators.
        Each cluster will contain senators whose votes are as similar to each other as possible.
        We'll need to specify the number of clusters we want upfront.
        Let's try 2 to see how that looks.
        '''
        # Create a kmeans model on our data, using 2 clusters.  random_state helps ensure that the algorithm returns the same results each time.
        kmeans_model = KMeans(n_clusters=2, random_state=1).fit(votes.iloc[:, 3:])
        # These are our fitted labels for clusters -- the first cluster has label 0, and the second has label 1.
        labels = kmeans_model.labels_
        print("==> labels")
        print(labels)
        #print("==> votes['Party']")
        #print(votes["Party"])
        # The clustering looks pretty good!
        # It's separated everyone into parties just based on voting history
        print("==> crosstab")
        print(pd.crosstab(labels, votes["Party"]))
        
        '''
        Step 2.5.
        append resulting labels to the original dataframe!
        '''
        votes['label'] = labels
        #print(votes)
        
        '''
        Step 3.
        Exploring people in the wrong cluster
        We can now find out which senators are in the "wrong" cluster.
        These senators are in the cluster associated with the opposite party.
        '''
        # Let's call these types of voters "oddballs" (why not?)
        # There aren't any republican oddballs
        democratic_oddballs = votes[(votes["label"] == 1) & (votes["Party"] == "D")]
        # It looks like Reid has abstained a lot, which changed his cluster.
        # Manchin seems like a genuine oddball voter.
        print("==> democratic_oddballs")
        print(democratic_oddballs["Name"])
        
        '''
        Step 3.5.
        republican oddballs
        '''
        republican_oddballs = votes[(votes["label"] == 0) & (votes["Party"] == "R")]
        print("==> republican_oddballs")
        print(republican_oddballs["Name"])
        
        '''
        Step 4.
        Plotting out the clusters
        Let's explore our clusters a little more by plotting them out.
        Each column of data is a dimension on a plot, and we can't visualize 15 dimensions.
        We'll use principal component analysis to compress the vote columns into two.
        Then, we can plot out all of our senators according to their votes, and shade them by their cluster.
        
        ===> Tutorial has several columns. In this case there is only one
        pca_2 = PCA(2)
        # Turn the vote data into two columns with PCA
        print(votes.iloc[:,3:18])
        plot_columns = pca_2.fit_transform(votes.iloc[:,3:18])
        # Plot senators based on the two dimensions, and shade by cluster label
        # You can see the plot by clicking "plots" to the bottom right
        plt.scatter(x=plot_columns[:,0], y=plot_columns[:,1], c=votes["label"])
        plt.show()
        plt.clf()
        '''
        pass


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testCluster']
    unittest.main()