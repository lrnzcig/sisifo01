'''
Created on 20/3/2015

@author: lorenzorubio
'''
import unittest
from time import time
import cx_Oracle
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from sklearn import metrics
from sklearn.cluster import KMeans
from sklearn.datasets import load_digits
from sklearn.decomposition import PCA
from sklearn.preprocessing import scale

from database import sisifo_connection

def clustering(self):
    conn = sisifo_connection.SisifoConnection().get()
    # user list
    users = pd.read_sql("select id, screen_name from tuser", conn, index_col='ID')
    print(users.shape)
    # add feature "follows"
    q = """
    select user_id as USER_ID, 0 as FOLLOWS
    from follower
    where followed_user_id = (select id
    from tuser
    where screen_name = 'ahorapodemos')
    """
    follows = pd.read_sql(q, conn, index_col='USER_ID')
    #print(follows.shape)
    users = users.join(follows)
    #print(follows[follows['FOLLOWS'] == 0].shape)
    print(users.shape)
    # query informs '0' when the user is following, but NaN when it is not
    users['FOLLOWS'].fillna(value=1, inplace=True)
    print("==> number of users not following")
    print(users[users['FOLLOWS'] == 1]['FOLLOWS'].count())
    print(follows['FOLLOWS'].count())

    # add feature "retweets"
    q = """
    select user_id as USER_ID, 1/(count(*) + 1) as RETWEETS
    from tweet
    where text like 'RT @ahorapodemos%'
    group by user_id
    """
    retweets = pd.read_sql(q, conn, index_col='USER_ID')
    #print(retweets.shape)
    users = users.join(retweets)
    print(users.shape)

    # query informs '1/(count(*) + 1)' when there is at least 1 retweet, but NaN when there is no retweet
    users['RETWEETS'].fillna(value=1, inplace=True)
    print("==> number of users with no retweets")
    print(users[users['RETWEETS'] == 1]['RETWEETS'].count())
    print(retweets['RETWEETS'].count())
    
    # add feature "usermentions"
    q = """
    select source_user_id as USER_ID, 1/(count(*) + 1) as MENTIONS
    from tusermention
    where target_user_id = (select id
    from tuser
    where screen_name = 'ahorapodemos')
    group by source_user_id
    """
    mentions = pd.read_sql(q, conn, index_col='USER_ID')
    #print(mentions.shape)
    users = users.join(mentions)
    print(users.shape)

    # query informs '1/(count(*) + 1)' when there is at least 1 mention, but NaN when there is no mention
    users['MENTIONS'].fillna(value=1, inplace=True)
    print("==> number of users with no mentions")
    print(users[users['MENTIONS'] == 1]['MENTIONS'].count())
    print(mentions['MENTIONS'].count())
    
    simpleKMeansPlot(users, False)
    
    # tweets of users of each label
    users0 = users[users['label'] == 0]
    print(users0.shape)
    print(users0[users0['FOLLOWS'] == 1]['SCREEN_NAME'].count())
    print(users0[users0['RETWEETS'] == 1]['SCREEN_NAME'].count())
    print(users0[users0['MENTIONS'] == 1]['SCREEN_NAME'].count())
    list0 = users0.index.values.tolist()
    print(list0)
    print(users0.index.values.shape)
    
    # TODO: 1) más estadísticas (2) colocar @ahora podemos en el 0 (3) comprobar más textos (4) comprobar usuarios conocidos
    
    users1 = users[users['label'] == 1]
    print(users1.shape)
    print(users1[users1['FOLLOWS'] == 1]['SCREEN_NAME'].count())
    print(users1[users1['RETWEETS'] == 1]['SCREEN_NAME'].count())
    print(users1[users1['MENTIONS'] == 1]['SCREEN_NAME'].count())
    list1 = users1.index.values.tolist()
    print(list1)
    print(users1.index.values.shape)
    
    #colorKMeansPlot(users.iloc[:, 1:], 2, 3)
    
    return

def simpleKMeansPlot(users, doPlot):
    # kmeans 2 clusters random initial
    #print(users.iloc[:,1:])
    kmeans_model = KMeans(n_clusters=2, random_state=1).fit(users.iloc[:, 1:])
    labels = kmeans_model.labels_
    users['label'] = labels

    if (doPlot == True):
        # simple plot
        pca_2 = PCA(2)
        #print(users.iloc[:,1:4])
        plot_columns = pca_2.fit_transform(users.iloc[:,1:4])
        #print(plot_columns[:,0])
        #print(plot_columns[:,1])
        #print(plot_columns.shape)
        plt.scatter(x=plot_columns[:,0], y=plot_columns[:,1], c=users["label"])
        plt.show()
        plt.clf()
    return

def colorKMeansPlot(data, n_clusters, n_features):
    #print('% 9s' % 'init'
    #      '    time  inertia    homo   compl  v-meas     ARI AMI  silhouette')
    
    # implies supervised?
    #bench_k_means(KMeans(init='k-means++', n_clusters=n_clusters, n_init=10),
    #              name="k-means++", data=data)
    
    #bench_k_means(KMeans(init='random', n_clusters=n_clusters, n_init=10),
    #              name="random", data=data)
    
    # in this case the seeding of the centers is deterministic, hence we run the
    # kmeans algorithm only once with n_init=1
    #pca = PCA(n_components=n_features).fit(data)
    #bench_k_means(KMeans(init=pca.components_, n_clusters=n_clusters, n_init=1),
    #              name="PCA-based",
    #              data=data)
    #print(79 * '_')
    
    ###############################################################################
    # Visualize the results on PCA-reduced data  
    reduced_data = PCA(n_components=2).fit_transform(data)
    kmeans = KMeans(init='k-means++', n_clusters=n_clusters, n_init=10)
    kmeans.fit(reduced_data)
    
    # Step size of the mesh. Decrease to increase the quality of the VQ.
    h = .02     # point in the mesh [x_min, m_max]x[y_min, y_max].
    
    # Plot the decision boundary. For that, we will assign a color to each
    x_min, x_max = reduced_data[:, 0].min() + 0.1, reduced_data[:, 0].max() - 0.1
    y_min, y_max = reduced_data[:, 1].min() + 0.1, reduced_data[:, 1].max() - 0.1
    xx, yy = np.meshgrid(np.arange(x_min, x_max, h), np.arange(y_min, y_max, h))
    
    # Obtain labels for each point in mesh. Use last trained model.
    Z = kmeans.predict(np.c_[xx.ravel(), yy.ravel()])
    
    # Put the result into a color plot
    Z = Z.reshape(xx.shape)
    plt.figure(1)
    plt.clf()
    plt.imshow(Z, interpolation='nearest',
               extent=(xx.min(), xx.max(), yy.min(), yy.max()),
               #cmap=plt.cm.Paired,
               aspect='auto', origin='lower')
    
    plt.plot(reduced_data[:, 0], reduced_data[:, 1], 'k.', markersize=2)
    # Plot the centroids as a white X
    centroids = kmeans.cluster_centers_
    plt.scatter(centroids[:, 0], centroids[:, 1],
                marker='x', s=169, linewidths=3,
                color='w', zorder=10)
    plt.title('K-means clustering on the digits dataset (PCA-reduced data)\n'
              'Centroids are marked with white cross')
    plt.xlim(x_min, x_max)
    plt.ylim(y_min, y_max)
    plt.xticks(())
    plt.yticks(())
    plt.show()
    return

# implies supervised?
# labels are the expected results
def bench_k_means(estimator, name, data, labels, sample_size):
    t0 = time()
    estimator.fit(data)
    print('% 9s   %.2fs    %i   %.3f   %.3f   %.3f   %.3f   %.3f    %.3f'
          % (name, (time() - t0), estimator.inertia_,
          metrics.homogeneity_score(labels, estimator.labels_),
          metrics.completeness_score(labels, estimator.labels_),
          metrics.v_measure_score(labels, estimator.labels_),
          metrics.adjusted_rand_score(labels, estimator.labels_),
          metrics.adjusted_mutual_info_score(labels,  estimator.labels_),
          metrics.silhouette_score(data, estimator.labels_,
                                   metric='euclidean',
                                   sample_size=sample_size)))
    return


class Test(unittest.TestCase):


    def testCluster(self):
        clustering(self)
        pass


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testCluster']
    unittest.main()