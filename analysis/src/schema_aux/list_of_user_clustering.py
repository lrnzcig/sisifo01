'''
Created on 7 de abr. de 2015

@author: lorenzorubio
'''
import pandas as pd
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from os.path import expanduser
import yaml

Base = declarative_base()

class ListOfUserClustering(Base):
    __tablename__ = 'list_of_users_clustering'
    id = Column(Integer, primary_key=True)
    cluster_label = Column(String(50))
    additional_label = Column(String(50))
    
    def __repr__(self):
        return "<ListOfUsersClustering(id='%s', cluster_label='%s', additional_label='%s')>" % (
            self.id, self.cluster_label, self.additional_label)

class Manager():
    '''
    writes & reads list of users
    '''
    
    '''
    # example: get list of users 
    users_list_mgr = luc.Manager()
    users = users_list_mgr.get()
    pure_set_0 = users.loc[users['cluster_label'] == '0'].loc[users['additional_label'] == 'pure_set']
    print(len(pure_set_0))
    '''
    
    '''
    # example: get tweets of that pure_set
    q = session.query(sch.Tweet).join(sch.User, sch.User.id == sch.Tweet.user_id) \
        .join(luc.ListOfUserClustering, luc.ListOfUserClustering.id == sch.User.id) \
        .filter(luc.ListOfUserClustering.cluster_label == '0').filter(luc.ListOfUserClustering.additional_label == 'pure_set')
    tweets_set_0 = pd.read_sql(q.statement, session.bind)
    print(tweets_set_0.shape)
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

    fdist_clean = [item for item in fdist.items() 
                   if item[0].lower() not in stopwords.words('spanish') and item[0].lower() not in stopwords_additional]
    
    fdist_clean_df = pd.DataFrame(fdist_clean, columns=['word', 'count'])
    fdist_clean_df.sort('count', ascending=False)[:50]
    '''
    
    def __init__(self, delete_all_cluster_lists=False):
        # export PATH or pass as an argument!
        properties = yaml.load(open(expanduser("~") + '/.sisifo/connection.properties'))
        database = properties['database']
        url = database['dialect'] + "://" + database['user'] + ":" + database['password'] + '@' + database['host'] + "/" + database['sid']
        self.engine = create_engine(url, echo=True)
        Base.metadata.create_all(self.engine)
        if (delete_all_cluster_lists == True):
            ## TODO should keep several clustering trials with labels for the clusters themselves
            Session = sessionmaker(bind=self.engine)
            session = Session()
            session.query(ListOfUserClustering).delete()
            session.commit()
    
    def dump(self, users_set, cluster_label, additional_label):
        Session = sessionmaker(bind=self.engine)
        session = Session()
        # NOTE: this could only be done once (replace drops the table)
        #us = pd.DataFrame(list(users_set))
        #us.to_sql(ListOfUserClustering.__tablename__, session.bind, if_exists='replace')
        for user in users_set:
            reg = ListOfUserClustering(id=user, cluster_label=cluster_label, additional_label=additional_label)
            session.add(reg)
        session.commit()
        
    def get(self):
        Session = sessionmaker(bind=self.engine)
        session = Session()
        q = session.query(ListOfUserClustering)
        return pd.read_sql(q.statement, session.bind)
        
        