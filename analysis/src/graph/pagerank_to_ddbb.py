# -*- coding: utf-8 -*-
'''
Created on 22 de sept. de 2015

@author: lorenzorubio
'''
import unittest
from datetime import datetime
from nltk.compat import UTC
from nltk.twitter.util import guess_path
from loader.tweet_loader_abstract import TweetLoaderAbstract
from schema_aux import user_page_rank_evolution as upre
import networkx as nx
import pandas as pd

import plotly.plotly as py
from plotly.graph_objs import *


class TweetGraphLoader(TweetLoaderAbstract):
    '''
    creates graph from tweet file
    '''
    
    def __init__(self, path, filename):
        self.tweets = TweetLoaderAbstract._get_tweet_dfs(self, path, filename, do_clean_duplicates=True, do_cleanup_carriage_returns=False)
        self.users = TweetLoaderAbstract._get_user_dfs(self, path, filename, do_clean_duplicates=True, do_cleanup_carriage_returns=False)
        self.preprocess_graph()

    def preprocess_graph(self):
        print(self.tweets.shape)
        print(self.users.shape)
        
        # edges with weights
        self.weights = self.tweets[self.tweets['retweet'] == 1].groupby(['user.id', 'retweeted_status.user.id']).size()
        self.weights.name = 'weight'
        print(self.weights[0:5])
        
        print(len(self.weights))
        print(len(self.weights.drop_duplicates()))
        
        for x in self.weights.iteritems():
            print(x)
            print(x[0][0])
            print(x[0][1])
            #print(x[1]['retweeted_user_id'])
            print(x[1])
            print(self.users.loc[x[0][0]])
            break

        # create date column using python datetime (in the json is plain text)
        self.tweets['created_at_date'] = self.tweets \
                .apply(lambda row : datetime.strptime(row['created_at'], '%a %b %d %H:%M:%S +0000 %Y').replace(tzinfo=UTC),
                       axis=1)
                
    def process_global_page_rank(self):       
        G = nx.DiGraph()
        G.add_nodes_from(int(x) for x in self.users.index)
        print(len(G))
        print(G[104010858])
        
        edges_list = [(int(x), int(y), z) for ((x,y),z) in self.weights.iteritems()]
        for x in edges_list:
            print(x)
            break
        
        G.add_weighted_edges_from(edges_list)
        print(len(G.edges()))
        
        pr = nx.pagerank(G, weight='weight')
        pagerank_result = pd.Series(pr, name="rank")
        pagerank_result.index.name = 'id'
        print(pagerank_result[1:5])
        print(len(pagerank_result))
        
        pagerank_df = pd.DataFrame(pagerank_result).join(self.users['screen_name'])
        print(pagerank_df[1:5])
        print(pagerank_df.sort(['rank'], ascending=False)[0:36])
        print(self.users.loc[3094195049])
        return pagerank_df
        
                
    def process_page_rank_evolution(self, global_page_rank_df, datetimes, year=2015, month=5, do_sliding_window=False):
        users_pr_evolution = self.users['screen_name']
        # add rank for the full graph
        users_pr_evolution = pd.DataFrame(users_pr_evolution).join(global_page_rank_df['rank'])
        
        num_tweets_evolution = []

        date_lower = None
        for day, time in datetimes:
            date_upper = datetime(year, month, day, time, 0, 0).replace(tzinfo=UTC)
            if do_sliding_window:
                pagerank_df_step, len_tweets_step = self.rank_before_date(date_upper, date_lower)
            else:
                pagerank_df_step, len_tweets_step = self.rank_before_date(date_upper, None)
            date_lower = date_upper
            
            users_pr_evolution = users_pr_evolution.join(pagerank_df_step['rank'], rsuffix=str(day)+','+str(time))
            num_tweets_evolution += [len_tweets_step]
            
        # from final date to end
        if do_sliding_window:
            pagerank_df_step, len_tweets_step = self.rank_before_date(None, date_lower)
            users_pr_evolution = users_pr_evolution.join(pagerank_df_step['rank'], rsuffix='final')
        
        print(num_tweets_evolution)
        print(users_pr_evolution.loc[282339186])
        print(users_pr_evolution.loc[20909329])
        
        return users_pr_evolution
        
        
    def get_edges_before_date(self, date_upper, date_lower=None):
        if date_upper:
            tweets_date = self.tweets[self.tweets['created_at_date'] < date_upper]
        else:
            tweets_date = self.tweets
        if date_lower:
            tweets_date = tweets_date[tweets_date['created_at_date'] > date_lower]
        weights = tweets_date[tweets_date['retweet'] == 1].groupby(['user.id', 'retweeted_status.user.id']).size()
        weights.name = 'weight'
        return weights, len(weights), len(tweets_date)
    
    def rank_before_date(self, date_upper, date_lower=None):
        G1=nx.DiGraph()
        G1.add_nodes_from(int(x) for x in self.users.index)
        weights1, len_edges, len_tweets = self.get_edges_before_date(date_upper, date_lower)
        G1.add_weighted_edges_from([(int(x), int(y), z) for ((x,y),z) in weights1.iteritems()])
        pr1 = nx.pagerank(G1, weight='weight')
        pagerank_result1 = pd.Series(pr1, name="rank")
        pagerank_result1.index.name = 'user_id'
        pagerank_df1 = pd.DataFrame(pagerank_result1).join(self.users['screen_name'])
        print("Date upper: {0}, date lower: {1}, edges: {2}, tweets: {3}".format(date_upper, date_lower, len_edges, len_tweets))
        print(pagerank_df1.sort(['rank'], ascending=False)[0:5])
        return pagerank_df1, len_tweets
    
    def get_scatter(self, user_id, df, x_axis, column_names):
        return Scatter(x=x_axis,
                        y=df.loc[user_id, column_names],
                        name=str(user_id))
        
    def graph_for_ids(self, ids, users_pr_evolution, x_axis, column_names):
        data_list = []
        for user_id in ids:
            data_list += [self.get_scatter(user_id, users_pr_evolution, x_axis, column_names)]
        
        data = Data(data_list)
        
        layout = Layout(
            title='evolución PageRank #desmontandoACS',
            showlegend=False,
            height=631,
            width=936,
            autosize=True
        )

        fig = Figure(data=data, layout=layout)
        py.image.ishow(fig)
        #py.image.save_as(fig, '/Users/lorenzorubio/Downloads/' + fig_name + '.png')
        

class Test(unittest.TestCase):


    def testName(self):
        pr_list_manager = upre.Manager(alchemy_echo=False, delete_all=False, user="almadraba")

        
        path = guess_path("twitter-files")
        filename = "tweets.20150506-180056.rest-desmontandoaciudadanos.json"
        ranker = TweetGraphLoader(path, filename)
        
        pr_list_manager.delete_all()
        
        global_page_rank_df = ranker.process_global_page_rank()
        
        datetimes = [(4, 9), (4, 12), (4, 15), (4, 18), (4, 21), (5, 0)]
        users_pr_evolution = ranker.process_page_rank_evolution(global_page_rank_df, datetimes, do_sliding_window=False)

        x_axis = ['d' + str(day) + ' ' + str(time) + 'h' for (day,time) in datetimes]
        x_axis += ['final']
        column_names = ['rank4,9', 'rank4,12', 'rank4,15', 'rank4,18', 'rank4,21', 'rank5,0', 'rank']
        
        for column in column_names:
            pr_list_manager.dump(users_pr_evolution.index, "full", column, users_pr_evolution[column])
        
        #top5
        ids = [282339186, 20909329, 341657886, 173665005, 187564239]
        
        # el top de ciudadanos
        ids = ids + [38643994]
        
        ranker.graph_for_ids(ids, users_pr_evolution, x_axis, column_names)

        column_names = ['rank4,9', 'rank4,12', 'rank4,15', 'rank4,18', 'rank4,21', 'rank5,0', 'rankfinal']
        
        users_pr_sliding_window = ranker.process_page_rank_evolution(global_page_rank_df, datetimes, do_sliding_window=True)

        for column in column_names:
            pr_list_manager.dump(users_pr_sliding_window.index, "window", column, users_pr_sliding_window[column])

        ranker.graph_for_ids(ids, users_pr_sliding_window, x_axis, column_names)

        pass


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()