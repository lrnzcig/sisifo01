# -*- coding: utf-8 -*-
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
from nltk.tokenize.casual import TweetTokenizer
from nltk.corpus import stopwords as nltk_stopwords
from nltk.stem import SnowballStemmer
from nltk.cluster import GAAClusterer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import AgglomerativeClustering
from sklearn.metrics import pairwise_distances
from time import time
from os.path import expanduser
import io
import matplotlib.pyplot as plt

class TweetClustering():
    
    '''
    #TODO move to another class
    it includes:
    - chars not processed ok by stemmer/tokenizer
    - common words in this context (review ????)
    '''
    stopwords_additional = ['@', ':', '?', '.', ',', '"', 'http', '#', '!', '!', '``', "''", "q", "d", "...",
                            "#desmontandoaciudadanos",
                            "#ciudadanos", "ciudadanos", "cs", "c's",
                            "ciutadans", '@ahorapodemos', '@albert_rivera', '@ciudadanoscs',
                            "albert", "rivera", ".."]
    
    def __init__(self):
        self.session = self.get_session()
        self.stemmer = SnowballStemmer('spanish')
        self.tokenizer = TweetTokenizer()
        self.stem_pairs = {}
        
        # load common words
        f = io.open(expanduser("~") + '/.sisifo/common_words_list.txt', mode='r', encoding='utf8')
        self.common_words = [self.stemmer.stem(text) for text in f.read().splitlines()]
    
        self.stopwords = self.common_words + [self.stemmer.stem(x) for x in nltk_stopwords.words('spanish')] + self.stopwords_additional
        
    '''
    gets tweets using a join to user clustering
    '''
    def get_tweets(self, cluster_label='belief_prop', additional_label='cs', rt_threshold=0):
        q = self.session.query(sch.Tweet) \
            .filter(sch.Tweet.retweet == False).filter(sch.Tweet.retweet_count >= rt_threshold) \
            .join(luc.ListOfUserClustering, luc.ListOfUserClustering.id == sch.Tweet.user_id) \
            .filter(luc.ListOfUserClustering.cluster_label == cluster_label) \
            .filter(luc.ListOfUserClustering.additional_label == additional_label)
            
        tweets = pd.read_sql(q.statement, self.session.bind)
        print("tweets.shape " + str(tweets.shape))
        return tweets

    def vectorize_tokenize(self, tweets, min_df=0):
        self.stem_pairs = {}
        vectorizer = TfidfVectorizer(ngram_range=(1,1),
                                     strip_accents='ascii',
                                     norm='l2',
                                     max_df=1.0,
                                     min_df=min_df,
                                     tokenizer=self.tokenize,
                                     stop_words=self.stopwords)
        r_tweets = vectorizer.fit_transform(tweets.loc[:, 'text'])
        print(r_tweets.shape)
        print(vectorizer.get_feature_names())
        return r_tweets, vectorizer.get_feature_names()
    
    def vectorize_vocabulary(self, tweets, vocabulary):
        vectorizer = TfidfVectorizer(ngram_range=(1,1),
                     strip_accents='ascii',
                     norm='l2',
                     vocabulary=vocabulary,
                     tokenizer=self.tokenize)

        r_tweets = vectorizer.fit_transform(tweets.loc[:, 'text'])
        print(r_tweets.shape)
        print(vectorizer.get_feature_names())
        return r_tweets, vectorizer.get_feature_names()
    
    def get_session(self):
        schema_mgr = sch.Manager(user='SISIFO01_AUCOMMAN', alchemy_echo=False)
        return schema_mgr.get_session()
    
    def tokenize(self, text):
        # removing some addional chars
        chars_to_remove = set(['´', "'"])
        text_chars = ''.join([c for c in text if c not in chars_to_remove])
        
        # reformatting - could be removed
        text_words = text_chars.replace("c s", "cs")
        
        # do not touch urls
        urls = [x for x in text_words.split() if x.startswith("http")]
        #print (urls)

        # tokenize and stem
        w_list = self.tokenizer.tokenize(text_words)
        no_url = [self.stem(x.lower()) for x in w_list if not x.startswith("http") and len(x) > 1]
        return no_url + urls

    def stem(self, text):     
        if 'jaja' in text:
            return 'jaja'
        if text.startswith('izq'):
            return 'izq'
        if text.startswith('catal'):
            return 'catal'
        if text.startswith('..'):
            return '..'
        if text.startswith('andalu'):
            return 'andalu'
        if text.startswith('calumni'):
            return 'calumni'
        if text.startswith('corrup'):
            return 'corrup'
        if text.startswith('democra'):
            return 'democra'
        if text.startswith('descalif'):
            return 'descalif'
        if text.startswith('deber'):
            return 'deber'
        if text.startswith('financ'):
            return 'financ'
        if 'podemos' in text and 'desmontandoapodemos' not in text and 'podemostienemiedo' not in text:
            return 'podemos'
        if text == 'hashtag' or text == 'ht' or text == 'hastag':
            return 'hashtag'
        if text.startswith('tranparen'):
            return 'transparent'
        if text == 'pablemos' or text == 'potemos' or text.startswith('podemit'):
            return 'pablemos'
        if text.startswith('nevi'):
            return 'nervi'
        if 'centro' in text and 'izq' in text:
            return 'centroizquierda'
        if 'ciudada' in text or 'ciutada' in text or text == 'cs' or text == "c's":
            if not text.startswith('#'):
                return 'ciudadan'
        if 'derech' in text:
            return 'derech'
        if 'falang' in text or 'falanj' in text or 'fascist' in text \
            or 'franquism' in text or 'franquist' in text or 'facha' in text or 'franco' in text \
            or 'nazi' in text:
            return 'falang'
        if 'racist' in text or 'racism' in text or 'xenof' in text:
            return 'racist'
        if 'psoe' in text:
            return text
        if 'pp' in text or 'psoe' in text:
            return 'pp'
        if 'venez' in text or text.startswith('chav') or text.startswith('bolivar')\
            or text.startswith('maduro'):
            return 'venez'
        if text == '#elcambiocs' or text == '#elcambiosensato' or text== '#cambiosensato' \
            or text == '#cuidadanoscumple':
            return '#cambiosensato'
        if text.startswith('#') or text.startswith('@'):
            return text
        try:
            number = int(text)
            if number < 1900:    
                return 'number'
            return number
        except ValueError:
            key = self.stemmer.stem(text)
            if key in self.stem_pairs:
                self.stem_pairs[key] += [text]
            else:
                self.stem_pairs[key] = [text]
            return key
    
    def run_clustering(self, X, n_clusters):
        model = AgglomerativeClustering(n_clusters=n_clusters, linkage="average", affinity="cosine")
    
        print("Clustering tweet content with %s" % model)
        t0 = time()
        result = model.fit_predict(X)
        print("done in %0.3fs" % (time() - t0))
        print()
        return result
    
    def interclass_distance_plot(self, Xdf, n_clusters):
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
    
    def show_features_sorted_by_counts(self, r_tweets, feature_list):
        features = pd.DataFrame(feature_list, columns=['feature'])
        features['count'] = [r_tweets.nonzero()[1].tolist().count(i) for i in features.index]
        print(features.sort(columns=['count'], ascending=False))
        
    def get_vector_column_counts(self, r_tweets):
        # r_tweets is a sparse matrix, counting number of columns that are not 0 per row
        return [r_tweets.nonzero()[0].tolist().count(i) for i in range(r_tweets.shape[0])]
    
    def check_columns_percent(self, r_tweets):
        column_counts_vector = self.get_vector_column_counts(r_tweets)
        if column_counts_vector.count(0) > r_tweets.shape[0] / 10:
            print("CAUTION! More than 10% of tweets have no columns. Percent: " + str(column_counts_vector.count(0) * 100 / r_tweets.shape[0]))
        return column_counts_vector

    def check_all_have_columns(self, r_tweets):
        column_counts_vector = self.get_vector_column_counts(r_tweets)
        if column_counts_vector.count(0) > 0:
            raise RuntimeError('ERROR!!! Should have selected tweets with features')
        return column_counts_vector
            
    def get_tweets_with_columns(self, tweets, r_tweets, column_counts_vector):
        return self._get_tweets_with_columns(tweets, r_tweets, column_counts_vector, True)
        
    def get_tweets_with_no_columns(self, tweets, r_tweets, column_counts_vector):
        return self._get_tweets_with_columns(tweets, r_tweets, column_counts_vector, False)        

    def _get_tweets_with_columns(self, tweets, r_tweets, column_counts_vector, with_columns):
        Xdf = pd.SparseDataFrame(r_tweets.toarray())
        Xdf['counts'] = column_counts_vector
        if with_columns:
            tweets_result = tweets.iloc[Xdf.loc[Xdf['counts'] > 0].index]
        else:
            tweets_result = tweets.iloc[Xdf.loc[Xdf['counts'] == 0].index]
        print(tweets_result.shape)
        return tweets_result
    
    def cluster(self, r_tweets, number_of_clusters=10):
        X = r_tweets.toarray()
        Xdf = pd.SparseDataFrame(X)
        print(Xdf.shape)
        
        clusterer = GAAClusterer(number_of_clusters)
        clusters = clusterer.cluster(X, assign_clusters=True, trace=False)
        Xdf['cluster'] = clusters
        return Xdf
        
    def cluster_summary(self, Xdf, number_of_clusters, tweets, feature_list, filename):
        # TODO change to setenv
        f2 = open('/Users/lorenzorubio/Downloads/' + filename, mode='w')
        for cluster in range(number_of_clusters):
            print("===> cluster " + str(cluster))
            cluster_tweets = Xdf.loc[Xdf['cluster'] == cluster]
            print("number of tweets: " + str(len(cluster_tweets.index)))
            for i in range(len(cluster_tweets.index)):
                f2.write('(' + str(cluster) + ',' + str(cluster_tweets.index[i]) + ')' 
                         + tweets.iloc[cluster_tweets.index[i]]['text'] + '\n')
            # analyze the most common used words
            totals = {}
            for i in range(len(cluster_tweets.index)):
                features = self._get_features_for_tweet(feature_list, Xdf.loc[cluster_tweets.index[i]])
                for f in features.iteritems():
                    feature = f[1]
                    if feature in totals:
                        totals[feature] += 1
                    else:
                        totals[feature] = 1
            totals_df = pd.DataFrame.from_dict(totals, 'index')
            totals_df.columns = ['key_counts']
            print(totals_df[totals_df['key_counts'] > 1].sort(columns=['key_counts'], ascending=False)) 
        f2.close()
    
    def _get_features_for_tweet(self, feature_list, x):
        features = pd.DataFrame(feature_list, columns=['feature'])
        return features.loc[x != 0, 'feature']
        
    def show_features_for_tweet(self, feature_list, x):
        print(self._get_features_for_tweet(feature_list, x))
                
class Test(unittest.TestCase):


    def testProcess(self):
        tc = TweetClustering()
        # unos pocos tweets de CS y coger las palabras más comunes
        tweets = tc.get_tweets(additional_label='cs', rt_threshold=0)
        r_tweets, feature_list = tc.vectorize_tokenize(tweets, min_df=11)
        tc.show_features_sorted_by_counts(r_tweets, feature_list)
        
        # hardcodes primera ejecución
        hardcodes = ['anticorrupcion', 'cambi', 'corrup', 'desmont',
                    'ilusion', 'pact', 'propon', 'propuest', 'sensat', 'venez']
        # hardcodes segunda ejecución
        hardcodes = ['anticorrupcion', 'cambi', 'corrup', 
                     'ilusion', 'pact', 'propon', 'propuest', 'sensat', 'venez',
                     '#cambiosensato', '#desmontandoeltt', 
                     'ataqu', 'campan', 'demagogi', 'derech', 'difam', 'hashtag', 'izq',
                     'mentir', 'mied', 'nervi', 'pablemos','podemos', 'pp', 'program', 
                     'regeneracion', 'solucion', 'transparent', 'tt']
        # hardcodes tercera ejecución (dudas!!!)
        hardcodes = ['anticorrupcion', 'cambi', 'corrup', 
                     'ilusion', 'pact', 'propon', 'propuest', 'sensat', 'venez',
                     '#cambiosensato', '#desmontandoeltt', 
                     'ataqu', 'campan', 'demagogi', 'derech', 'difam', 'hashtag', 'izq',
                     'mentir', 'mied', 'nervi', 'pablemos','podemos', 'pp', 'program', 
                     'regeneracion', 'solucion', 'transparent', 'tt',
                     'constru', 'honradez', 'dialog', 'coherent']
        # hardcodes cuarta ejecución (más dudas!!!)
        hardcodes = ['anticorrupcion', 'cambi', 'corrup', 
                     'ilusion', 'pact', 'propon', 'propuest', 'sensat', 'venez',
                     '#cambiosensato', '#desmontandoeltt', 
                     'ataqu', 'campan', 'demagogi', 'derech', 'difam', 'hashtag', 'izq',
                     'mentir', 'mied', 'nervi', 'pablemos','podemos', 'pp', 'program', 
                     'regeneracion', 'solucion', 'transparent', 'tt',
                     'constru', 'honradez', 'dialog', 'coherent',
                     'goebbels', 'psoe']

        
        # vectorizar usando vocabulario=hardcodes
        r_tweets, feature_list_hard = tc.vectorize_vocabulary(tweets, hardcodes)
        column_counts_vector = tc.check_columns_percent(r_tweets)
        
        # coger solo los tweets que tienen alguno de los hardcodes
        tweets_hard = tc.get_tweets_with_columns(tweets, r_tweets, column_counts_vector)
        
        # vectorizar otra vez, solo estos tweets
        r_tweets_hard, feature_list_hard = tc.vectorize_vocabulary(tweets_hard, hardcodes)
        tc.check_all_have_columns(r_tweets_hard)
        
        # clusters
        number_of_clusters = 20
        Xdf_hard = tc.cluster(r_tweets_hard, number_of_clusters)
        
        tc.cluster_summary(Xdf_hard, number_of_clusters, tweets_hard, feature_list_hard, 'clusters_hard1.log')
        print(tweets_hard.shape)
        # ver las features de un tweet
        tc.show_features_for_tweet(feature_list_hard, Xdf_hard.loc[41])
        
        # vectorizar el resto de tweets
        print(tweets.shape)
        tweets_rest = tc.get_tweets_with_no_columns(tweets, r_tweets, column_counts_vector)
        print(tweets_rest.shape)
        r_tweets_rest, feature_list_rest = tc.vectorize_tokenize(tweets_rest, min_df=2)
        tc.show_features_sorted_by_counts(r_tweets_rest, feature_list_rest)
        
        # ver el origen de una palabra que ha pasado por stem
        #tc.stem_pairs['constru']
        
        pass


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testProcess']
    unittest.main()