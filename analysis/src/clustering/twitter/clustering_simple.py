'''
Created on 20/3/2015

@author: lorenzorubio
'''
# -*- coding: utf-8 -*-
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

user_id_feature_name = 'USER_ID'
screen_name_feature_name = 'SCREEN_NAME'

def init_users_dataframe(conn, verbose=True):
    users = pd.read_sql("select id, screen_name from tuser", conn, index_col='ID')
    if (verbose == True):
        print("# users to analyse: " + str(users[screen_name_feature_name].size))
    return users


def get_column_for_follows(user_screen_name, feature_sufix):
    return 'FOLLOWS' + feature_sufix
    
def followers_stats(df, user_screen_name, feature_sufix):
    dataframe_column_for_follows = get_column_for_follows(user_screen_name, feature_sufix)
    print("==> followers of @" + user_screen_name)
    print("number of users following     : " + str(df.loc[df[dataframe_column_for_follows] != 1, dataframe_column_for_follows].count()))
    print("number of users not following : " + str(df.loc[df[dataframe_column_for_follows] == 1, dataframe_column_for_follows].count()))

def get_column_for_retweets(user_screen_name, feature_sufix):
    return 'RETWEETS' + feature_sufix
    
def retweets_stats(df, user_screen_name, feature_sufix):
    dataframe_column_for_retweets = get_column_for_retweets(user_screen_name, feature_sufix)
    print("==> retweets of @" + user_screen_name)
    print("number of users with retweets : " + str(df.loc[df[dataframe_column_for_retweets] != 1, dataframe_column_for_retweets].count()))
    print("number of users w no retweets : " + str(df.loc[df[dataframe_column_for_retweets] == 1, dataframe_column_for_retweets].count()))

def get_column_for_mentions(user_screen_name, feature_sufix):
    return 'MENTIONS' + feature_sufix
    
def mentions_stats(df, user_screen_name, feature_sufix):
    dataframe_column_for_mentions = get_column_for_mentions(user_screen_name, feature_sufix)
    print("==> mentions of @" + user_screen_name)
    print("number of users with mentions : " + str(df.loc[df[dataframe_column_for_mentions] != 1, dataframe_column_for_mentions].count()))
    print("number of users w no mentions : " + str(df.loc[df[dataframe_column_for_mentions] == 1, dataframe_column_for_mentions].count()))


def prepare_features_for_user(user_screen_name, conn, feature_sufix, users, verbose=True):
    '''
    Calculates features for one user and adds them to dataframe users
    Input:
        user_screen_name
        conn: connection to database (cx_Oracle object)
        users: dataframe with ID = user_id
        verbose: if True shows messages
    Output:
        users: dataframe with added features
    '''
    # user list
    # add feature "follows"
    followers_query = """
    select user_id as {user_id_feature_name}, 0 as {dataframe_column_for_follows}
    from follower
    where followed_user_id = (select id
    from tuser
    where screen_name = '{user_screen_name}')
    """
    dataframe_column_for_follows = get_column_for_follows(user_screen_name, feature_sufix)
    q = followers_query.format(
        user_screen_name = user_screen_name,
        user_id_feature_name = user_id_feature_name,
        dataframe_column_for_follows = dataframe_column_for_follows
    )
    follows = pd.read_sql(q, conn, index_col=user_id_feature_name)
    #print(follows.shape)
    users = users.join(follows)
    #print(follows[follows['FOLLOWS'] == 0].shape)
    #print(users)
    
    # query informs '0' when the user is following, but NaN when it is not
    users[dataframe_column_for_follows].fillna(value=1, inplace=True)
    if (verbose == True):
        followers_stats(users, user_screen_name, feature_sufix)
        
    # add feature "retweets"
    retweets_query = """
    select user_id as {user_id_feature_name}, 1/(count(*) + 1) as {dataframe_column_for_retweets}
    from tweet
    where text like 'RT @{user_screen_name}%'
    group by user_id
    """
    dataframe_column_for_retweets = get_column_for_retweets(user_screen_name, feature_sufix)    
    q = retweets_query.format(
        user_screen_name = user_screen_name,
        user_id_feature_name = user_id_feature_name,
        dataframe_column_for_retweets = dataframe_column_for_retweets        
    )
    retweets = pd.read_sql(q, conn, index_col=user_id_feature_name)
    #print(retweets.shape)
    users = users.join(retweets)
    #print(users.shape)

    # query informs '1/(count(*) + 1)' when there is at least 1 retweet, but NaN when there is no retweet
    users[dataframe_column_for_retweets].fillna(value=1, inplace=True)
    if (verbose == True):
        retweets_stats(users, user_screen_name, feature_sufix)
    
    # add feature "usermentions"
    mentions_query = """
    select source_user_id as {user_id_feature_name}, 1/(count(*) + 1) as {dataframe_column_for_mentions}
    from tusermention
    where target_user_id = (select id
    from tuser
    where screen_name = '{user_screen_name}')
    group by source_user_id
    """
    dataframe_column_for_mentions = get_column_for_mentions(user_screen_name, feature_sufix)    
    q = mentions_query.format(
        user_screen_name = user_screen_name,
        user_id_feature_name = user_id_feature_name,
        dataframe_column_for_mentions = dataframe_column_for_mentions        
    )
    mentions = pd.read_sql(q, conn, index_col=user_id_feature_name)
    #print(mentions.shape)
    users = users.join(mentions)
    #print(users.shape)

    # query informs '1/(count(*) + 1)' when there is at least 1 mention, but NaN when there is no mention
    users[dataframe_column_for_mentions].fillna(value=1, inplace=True)
    if (verbose == True):
        mentions_stats(users, user_screen_name, feature_sufix)
        
    # put the ref user in coordinates (0, 0)
    for column in users.columns:
        if (column != screen_name_feature_name):
            users.loc[users[screen_name_feature_name] == user_screen_name, column] = 0
    
    #print(users)
    #print(users[users[screen_name_feature_name] == user_screen_name])
    return users

def cluster_stats(df, label, users, verbose=False):
    '''
    Shows statistics
    - df: dataframe containing the cluster
    - label
    - users: list of reference users
    '''
    print('=======================================')
    print('==> gropup label ' + label)
    print('size: ' + str(df[screen_name_feature_name].size))
    for user_screen_name in users:
        if (find_user_in_df(df, user_screen_name) != 0):
            print('includes reference users: ' + user_screen_name)
        feature_sufix = "_" + user_screen_name.upper()
        followers_stats(df, user_screen_name, feature_sufix)
        retweets_stats(df, user_screen_name, feature_sufix)
        mentions_stats(df, user_screen_name, feature_sufix)
        dataframe_column_for_follows = get_column_for_follows(user_screen_name, feature_sufix)
        if (df.loc[df[dataframe_column_for_follows] == 0, dataframe_column_for_follows].count() > 0):
            print('members following @' + user_screen_name + ': ')
            print(df.loc[df[dataframe_column_for_follows] == 0, screen_name_feature_name].tolist())
    if (verbose == True):      
        print('members indexes: ')
        print(df.index.values.tolist())
        print('members screen names:')
        print(df[screen_name_feature_name].tolist())
    print('=======================================')

def find_user_in_df(df, user_screen_name):
    return len(df.loc[df[screen_name_feature_name] == user_screen_name, screen_name_feature_name])

def clustering(users):
    '''
    Input
        users: list of users' screen_name of interest
    '''
    conn = sisifo_connection.SisifoConnection().get()
    
    users_df = init_users_dataframe(conn, True)
    for user_screen_name in users:
        users_df = prepare_features_for_user(user_screen_name, conn, "_" + user_screen_name.upper(), users_df, True)
     
    # kmeans 2 clusters random initial
    #print(users.iloc[:,1:])
    kmeans_model = KMeans(n_clusters=2, random_state=1).fit(users_df.iloc[:, 1:])
    labels = kmeans_model.labels_
    users_df['label'] = labels


    # tweets of users of each label
    users0 = users_df[users_df['label'] == 0]
    cluster_stats(users0, '0', users)

    users1 = users_df[users_df['label'] == 1]
    cluster_stats(users1, '1', users)
    
    # TODO: 
    # (1) more (meaningful) graphs 
    # (2) check more results manually
    
    simpleKMeansPlot(users_df, False)    
    #colorKMeansPlot(users.iloc[:, 1:], 2, 3)
    
    return

def simpleKMeansPlot(users, doPlot):
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
        clustering(['ahorapodemos'])
        pass


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testCluster']
    unittest.main()