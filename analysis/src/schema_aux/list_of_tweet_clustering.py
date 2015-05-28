'''
Created on 7 de abr. de 2015

@author: lorenzorubio
'''
import pandas as pd
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import schema_aux.utils as utils

Base = declarative_base()

class ListOfTweetClustering(Base):
    __tablename__ = 'list_of_tweets_clustering'
    id = Column(Integer, primary_key=True)
    cluster_label = Column(String(50))
    additional_label = Column(String(50))
    
    def __repr__(self):
        return "<ListOfTweetsClustering(id='%s', cluster_label='%s', additional_label='%s')>" % (
            self.id, self.cluster_label, self.additional_label)

class Manager():
    '''
    writes & reads list of users
    '''
    def __init__(self, user=None, alchemy_echo=True, delete_all_cluster_lists=False):
        url = utils.get_database_url_sql_alchemy(user, alchemy_echo)
        self.engine = create_engine(url, echo=False)
        Base.metadata.create_all(self.engine)
        if (delete_all_cluster_lists == True):
            ## TODO should keep several clustering trials with labels for the clusters themselves
            Session = sessionmaker(bind=self.engine)
            session = Session()
            session.query(ListOfTweetClustering).delete()
            session.commit()
    
    def dump(self, tweet_ids_set, cluster_label, additional_label):
        Session = sessionmaker(bind=self.engine)
        session = Session()
        for user in tweet_ids_set:
            reg = ListOfTweetClustering(id=user, cluster_label=cluster_label, additional_label=additional_label)
            session.add(reg)
        session.commit()
        
    def get(self):
        Session = sessionmaker(bind=self.engine)
        session = Session()
        q = session.query(ListOfTweetClustering)
        return pd.read_sql(q.statement, session.bind)
        
        