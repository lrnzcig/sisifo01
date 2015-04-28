'''
Created on 14 de abr. de 2015

@author: lorenzorubio
'''
from __future__ import division
import unittest
import pandas as pd
import numpy as np
from schema_aux import list_of_user_clustering as luc
from schema_aux import twitter_schema as sch
from nltk.tokenize import word_tokenize
from nltk.probability import FreqDist
from nltk.corpus import stopwords
#from nltk.stem import SnowballStemmer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from sklearn import metrics
from time import time
from sklearn.cluster import AgglomerativeClustering
from sklearn.metrics import pairwise_distances
import matplotlib.pyplot as plt

def run_clustering(X, n_clusters):
    model = AgglomerativeClustering(n_clusters=n_clusters, linkage="ward", affinity="cosine")

    print("Clustering tweet content with %s" % model)
    t0 = time()
    model.fit(X)
    print("done in %0.3fs" % (time() - t0))
    print()
    return model

def interclass_distance_plot(Xdf, n_clusters):
    # interclass distances plot
    # from http://scikit-learn.org/stable/auto_examples/cluster/plot_agglomerative_clustering_metrics.html#example-cluster-plot-agglomerative-clustering-metrics-py
    avg_dist = np.zeros((n_clusters, n_clusters))

    plt.subplot(2, 5, n_clusters-1)
    for i in range(n_clusters):
        for j in range(n_clusters):
            avg_dist[i, j] = pairwise_distances(Xdf.loc[Xdf['label'] == i], Xdf.loc[Xdf['label'] == j],
                                                metric='cosine').mean()
    avg_dist /= avg_dist.max()
    for i in range(n_clusters):
        for j in range(n_clusters):
            plt.text(i, j, '%5.3f' % avg_dist[i, j],
                 verticalalignment='center',
                 horizontalalignment='center')

    plt.imshow(avg_dist, interpolation='nearest', #cmap=plt.cm.gnuplot2,
               vmin=0)
    plt.xticks(range(n_clusters), range(n_clusters))
    plt.yticks(range(n_clusters), range(n_clusters))
    plt.colorbar()
    plt.suptitle("Interclass cosine distances")
    plt.tight_layout()

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
    
    # all tweets
    q = session.query(sch.Tweet)
    all_tweets = pd.read_sql(q.statement, session.bind)
    print(all_tweets.shape)
    
    tweet1_tokens = word_tokenize(tweets_set_0.loc[1, 'text'])
    print(sorted(set(tweet1_tokens)))
    
    corpus = [word for tweet in tweets_set_0.loc[:, 'text'] for word in word_tokenize(tweet)]
    print(sorted(set(corpus)))
    print(len(set(corpus)))
    
    # diversity (how many times a word is repeated on average)
    print("diversity", len(corpus) / len(set(corpus)))
    
    # frequency distribution
    fdist = FreqDist(corpus)
    #fdist.plot(50, cumulative=True)
    
    print(fdist['ahorapodemos'])
    
    # remove stopwords to fdist (it could also be done to corpus instead
    stopwords_additional = ['@', ':', '?', '.', ',', '"', 'http', '#', '!', '!', '``', "''", "q", "d", "..."]

    fdist_clean = [item for item in fdist.items() 
                   if item[0].lower() not in stopwords.words('spanish') and item[0].lower() not in stopwords_additional]
    
    fdist_clean_df = pd.DataFrame(fdist_clean, columns=['word', 'count'])
    fdist_clean_df.sort('count', ascending=False)[:50]
    
    '''
    stemmer = SnowballStemmer('spanish')
    def tokenize(text):
        tokens = word_tokenize(text)
        return [stemmer.stem(token) for token in tokens]
        '''
    def tokenize(text):
        return word_tokenize(text, 'spanish')

    vectorizer = TfidfVectorizer(tokenizer=tokenize, stop_words=stopwords.words('spanish') + stopwords_additional)
    r_tweets_set_0 = vectorizer.fit_transform(tweets_set_0.loc[:, 'text'])
    print(r_tweets_set_0.shape)

    feature_names = vectorizer.get_feature_names()
    print(feature_names[:15])
    print(len(feature_names))
    print(len(r_tweets_set_0.nonzero()))
    '''
    for col in r_tweets_set_0.nonzero()[1]:
        # results is matrix with all the tweets, results[0,... gives values for tweet0
        print(feature_names[col], ' - ', r_tweets_set_0[0, col])
        return
        '''
    
    print(type(r_tweets_set_0))
    # not good for performance; output of vectorizer is scipy.sparse.csr_matrix (compressed)
    # could be converted to pd.SparseDataFrame
    
    '''
    X = r_tweets_set_0.toarray()
    print(r_tweets_set_0[:10])
    print(X[:10])
    '''


    r_all_tweets = vectorizer.fit_transform(all_tweets.loc[:25000, 'text'])
    print("all tweeets shape ", r_all_tweets.shape)
    
    X = r_all_tweets.toarray()
    print(r_all_tweets[:10])
    print(X[:10])
    
    Xdf = pd.SparseDataFrame(X)
    # from http://stackoverflow.com/questions/17818783/populate-a-pandas-sparsedataframe-from-a-scipy-sparse-matrix
    # very slow and I don't think it does anything different
    #Xdf = pd.SparseDataFrame([ pd.SparseSeries(results_corpus[i].toarray().ravel()) 
    #                              for i in np.arange(results_corpus.shape[0]) ])
    print(Xdf.shape)
    
    models = []
    plt.figure(figsize=(15, 9))
    for n_clusters in range(2,4):
        model = run_clustering(X, n_clusters)
        models += [model]
        
        Xdf['label'] = model.labels_
        #print(Xdf[:2])
        
        interclass_distance_plot(Xdf, n_clusters)
        
        clusters = []
        for label in range(n_clusters):
            print("===> label " + str(label))
            cluster = Xdf.loc[Xdf['label'] == label]
            clusters += [cluster]
            print("number of tweets: " + str(len(cluster.index)))
            print("sample tweets")
            for i in range(len(cluster.index)):
                if len(cluster.index) == i:
                    break
                print(all_tweets.loc[cluster.index[i], 'text'])
            
    plt.show()

    '''    
    km = KMeans(n_clusters=2, init='k-means++', max_iter=100, n_init=1,
                verbose=True)

    print("Clustering tweet content with %s" % km)pupu
    t0 = time()
    km.fit(r_tweets_set_0)
    print("done in %0.3fs" % (time() - t0))
    print()
    
    print(km.labels_.shape)
    
    tweets_set_0['label'] = km.labels_
    print(len(tweets_set_0.loc[tweets_set_0['label'] == 0]))
    print(len(tweets_set_0.loc[tweets_set_0['label'] == 1]))
    #print("Homogeneity: %0.3f" % metrics.homogeneity_score(labels, km.labels_))
    #print("Completeness: %0.3f" % metrics.completeness_score(labels, km.labels_))
    #print("V-measure: %0.3f" % metrics.v_measure_score(labels, km.labels_))
    #print("Adjusted Rand-Index: %.3f"
    #      % metrics.adjusted_rand_score(labels, km.labels_))
    print("Silhouette Coefficient: %0.3f"
      % metrics.silhouette_score(r_tweets_set_0, km.labels_, sample_size=1000))
    '''
    
class Test(unittest.TestCase):


    def testProcess(self):
        process_clusters()
        pass


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testProcess']
    unittest.main()