'''
Created on 20/3/2015

@author: lorenzorubio
'''
# -*- coding: utf-8 -*-
from time import time
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from sklearn import metrics
from sklearn.cluster import KMeans
from sklearn.datasets import load_digits
from sklearn.decomposition import PCA
from sklearn.preprocessing import scale

from database import sisifo_connection
from schema_aux import list_of_user_clustering
from clustering.twitter import utils

class Clustering():
    '''
    simple aproach to clustering
    '''
    def __init__(self, users, number_of_clusters):
        self.ref_users = users
        self.number_of_clusters = number_of_clusters
        self.conn = sisifo_connection.SisifoConnection().get()
        self.users = self.init_users_dataframe()
        self.dumper = list_of_user_clustering.Manager(delete_all_cluster_lists=True) #__init__ of dumper removes list

    
    def init_users_dataframe(self, verbose=True):
        '''
        gets all users form database
        '''
        users = pd.read_sql("select id as ID, upper(screen_name) as SCREEN_NAME from tuser", self.conn, index_col='ID')
        if (verbose == True):
            print("# users to analyse: " + str(users[utils.screen_name_feature_name].size))
        return users
    
    def clustering(self, writeFiles=True):
        '''
        Runs the clustering
        Input
            users: list of users' screen_name of interest
        '''
        if (writeFiles == True):
            # write all users to file (get friends & followers)
            np.savetxt('users.csv', self.users.index, delimiter=',', fmt='%s')
        
        # prepare features for all of the relevant users
        for user_screen_name in self.ref_users:
            self.prepare_features_for_user(user_screen_name, "_" + user_screen_name.upper(), False)
         
        # kmeans 2 clusters random initial
        #print(users.iloc[:,1:])
        kmeans_model = KMeans(n_clusters=self.number_of_clusters, random_state=1).fit(self.users.iloc[:, 1:])
        labels = kmeans_model.labels_
        self.users['label'] = labels
    
    
        # tweets of users of each label
        for i in range(self.number_of_clusters):
            self.cluster_stats(self.users[self.users['label'] == i], str(i), persist=writeFiles)
    
        
        # TODO: 
        # (1) more (meaningful) graphs         
        #simpleKMeansPlot(users_df, False)    
        #colorKMeansPlot(users.iloc[:, 1:], 2, 3)
        
        return

    
    
    
    def prepare_features_for_user(self, user_screen_name, feature_sufix, verbose=True):
        '''
        Calculates features for one user and adds them to dataframe users
        Input:
            user_screen_name
            verbose: if True shows messages
            self.conn: connection to database (cx_Oracle object)
            self.users: dataframe with ID = user_id
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
        where upper(screen_name) = upper('{user_screen_name}'))
        """
        dataframe_column_for_follows = utils.get_column_for_follows(user_screen_name, feature_sufix)
        q = followers_query.format(
            user_screen_name = user_screen_name,
            user_id_feature_name = utils.user_id_feature_name,
            dataframe_column_for_follows = dataframe_column_for_follows
        )
        follows = pd.read_sql(q, self.conn, index_col=utils.user_id_feature_name)
        #print(follows.shape)
        self.users = self.users.join(follows)
        #print(follows[follows['FOLLOWS'] == 0].shape)
        #print(users)
        
        # query informs '0' when the user is following, but NaN when it is not
        self.users[dataframe_column_for_follows].fillna(value=1, inplace=True)
        if (verbose == True):
            utils.followers_stats(self.users, user_screen_name, feature_sufix)
            
        # add feature "retweets"
        retweets_query = """
        select user_id as {user_id_feature_name}, 1/(count(*) + 1) as {dataframe_column_for_retweets}
        from tweet
        where upper(text) like upper('RT @{user_screen_name}%')
        group by user_id
        """
        dataframe_column_for_retweets = utils.get_column_for_retweets(user_screen_name, feature_sufix)    
        q = retweets_query.format(
            user_screen_name = user_screen_name,
            user_id_feature_name = utils.user_id_feature_name,
            dataframe_column_for_retweets = dataframe_column_for_retweets        
        )
        retweets = pd.read_sql(q, self.conn, index_col=utils.user_id_feature_name)
        #print(retweets.shape)
        self.users = self.users.join(retweets)
        #print(users.shape)
    
        # query informs '1/(count(*) + 1)' when there is at least 1 retweet, but NaN when there is no retweet
        self.users[dataframe_column_for_retweets].fillna(value=1, inplace=True)
        if (verbose == True):
            self.retweets_stats(self.users, user_screen_name, feature_sufix)
        
        # add feature "usermentions"
        mentions_query = """
        select source_user_id as {user_id_feature_name}, 1/(count(*) + 1) as {dataframe_column_for_mentions}
        from tusermention
        where target_user_id = (select id
        from tuser
        where upper(screen_name) = upper('{user_screen_name}'))
        group by source_user_id
        """
        dataframe_column_for_mentions = utils.get_column_for_mentions(user_screen_name, feature_sufix)    
        q = mentions_query.format(
            user_screen_name = user_screen_name,
            user_id_feature_name = utils.user_id_feature_name,
            dataframe_column_for_mentions = dataframe_column_for_mentions        
        )
        mentions = pd.read_sql(q, self.conn, index_col=utils.user_id_feature_name)
        #print(mentions.shape)
        self.users = self.users.join(mentions)
        #print(users.shape)
    
        # query informs '1/(count(*) + 1)' when there is at least 1 mention, but NaN when there is no mention
        self.users[dataframe_column_for_mentions].fillna(value=1, inplace=True)
        if (verbose == True):
            self.mentions_stats(self.users, user_screen_name, feature_sufix)
            
        # put the ref user in coordinates (0, 0)
        for column in self.users.columns:
            if (column != utils.screen_name_feature_name):
                self.users.loc[self.users[utils.screen_name_feature_name] == user_screen_name, column] = 0
        
        #print(users)
        #print(users[users[screen_name_feature_name] == user_screen_name])
        return
    
    def cluster_stats(self, df, label, verbose=True, persist=True, plots=False):
        '''
        Shows statistics
        - df: dataframe containing the cluster
        - label
        '''
        print('=======================================')
        print('==> gropup label ' + label)
        print('size: ' + str(df[utils.screen_name_feature_name].size))
        ref_users_included = []
        followers_of_ref_users = set()
        '''
        any of the reference users is present or not
        '''
        for user_screen_name in self.ref_users:
            if (utils.find_user_in_df(df, user_screen_name) != 0):
                ref_users_included.append(user_screen_name)
                print('includes reference users: ' + user_screen_name)
        '''
        members of the cluster following any of the reference users
        '''
        for ref_user in ref_users_included:
            if (verbose == True) & (utils.there_are_followers(df, ref_user)):
                print('members following @' + ref_user + ': ')
                followers = utils.get_followers(df,ref_user)
                followers_of_ref_users.update(followers.tolist())
                print(followers.tolist())
        '''
        members of the cluster following some of the other reference users, but not the reference users of the cluster
        '''
        for user in set(self.ref_users) - set(ref_users_included):
            if (utils.there_are_followers(df, user)):
                followers = utils.get_followers(df,user)
                followers_removing_ref = set(followers.tolist()) - followers_of_ref_users
                if (len(followers_removing_ref) > 0):     
                    print('members following @' + user + ' (and not following reference users of the cluster): ')
                    print(followers_removing_ref)
        
        if (plots == True):
            self.plot_histograms(df, label, self.ref_users)
        
        if (verbose == True):
            for user_screen_name in self.ref_users:
                feature_sufix = "_" + user_screen_name.upper()
                utils.followers_stats(df, user_screen_name, feature_sufix)
                utils.retweets_stats(df, user_screen_name, feature_sufix, summary=False)
                utils.mentions_stats(df, user_screen_name, feature_sufix)
                '''
                can't see anything on these, better out to csv
                print('members indexes: ')
                print(df.index.values.tolist())
                print('members screen names:')
                print(df[screen_name_feature_name].tolist())
                '''
        print('=======================================')
    
        if (persist == True):
            if (ref_users_included):
                self.persist_distributed_users(df, label, ref_users_included)
            else:
                self.persist_all_of_users(df, label)
                
    
    def persist_distributed_users(self, df, label, cluster_ref_users):
        users_pure_set, users_white_set, users_no_set = self.distribute_users(df, cluster_ref_users)
        self.dumper.dump(users_pure_set, label, 'pure_set')
        self.dumper.dump(users_white_set, label, 'white_set')
        self.dumper.dump(users_no_set, label, 'no_set')
        
    def persist_all_of_users(self, df, label):
        self.dumper.dump(set(df.index), label, 'all')
    
    def distribute_users(self, df, cluster_ref_users):
        '''
        3 files
        - pure
            have retweets of cluster_ref_users
            have no retweets of other ref_users
        - white
            have no retweets of other ref_users
        - no
            have retweets of other ref_users
        '''
        users_no_set = set()
        for user in set(self.ref_users) - set(cluster_ref_users):
            users_no_set.update(utils.get_users_with_retweets(df, user, "_" + user.upper()).index)
            
        remaining_set = set(df.index) - users_no_set
        users_pure_set = set()
        for user in set(cluster_ref_users):
            users_pure_set.update(utils.get_users_with_retweets(df.loc[remaining_set], user, "_" + user.upper()).index)
                
        users_white_set = remaining_set - users_pure_set
        return users_pure_set, users_white_set, users_no_set
            
    
    def plot_histograms(self, df, label, users):
        plt.figure(figsize=(16,9))
        
        position = 1
        for user in users:
            plt.subplot(2, 5, position)
            dataframe_column_for_retweets = utils.get_column_for_retweets(user, "_" + user.upper())
            for_hist = pd.DataFrame((1 / df[dataframe_column_for_retweets]) - 1)
            plt.hist(for_hist[for_hist[dataframe_column_for_retweets] > 0].values, bins=20)
            plt.title("Retweets " + user)
            position += 1
    
        plt.show()
    
    
    
    def simpleKMeansPlot(self, users, doPlot):
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
    
    def colorKMeansPlot(self, data, n_clusters, n_features):
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
    def bench_k_means(self, estimator, name, data, labels, sample_size):
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


