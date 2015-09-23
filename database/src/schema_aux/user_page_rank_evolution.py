'''
Created on 22 de sept. de 2015

@author: lorenzorubio
'''
import pandas as pd
from sqlalchemy import Column, Integer, String, Float, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import schema_aux.utils as utils

Base = declarative_base()

class UserPageRankEvolution(Base):
    __tablename__ = 'user_page_rank_evolution'
    user_id = Column(Integer, primary_key=True)
    rank_exec_id = Column(String(256), primary_key=True)
    rank_step_id = Column(String(256), primary_key=True)
    rank = Column(Float)
    
    def __repr__(self):
        return "<UserPageRankEvolution(user_id='%s', rank_exec_id='%s', rank_step_id='%s')>" % (
            self.user_id, self.rank_exec_id, self.rank_step_id)

class Manager():
    '''
    manager for list of users by pagerank
    '''
    def __init__(self, user=None, alchemy_echo=True, delete_all=False):
        # export PATH or pass as an argument!
        url = utils.get_database_url_sql_alchemy(user, alchemy_echo)
        self.engine = create_engine(url, echo=True)
        Base.metadata.create_all(self.engine)
        if (delete_all == True):
            self.delete_all()
            
    def delete_all(self):
        ## TODO should keep several clustering trials with labels for the clusters themselves
        Session = sessionmaker(bind=self.engine)
        session = Session()
        session.query(UserPageRankEvolution).delete()
        session.commit()
    
    def dump(self, users_set, rank_exec_id, rank_step_id, rank_df):
        Session = sessionmaker(bind=self.engine)
        session = Session()
        # NOTE: this could only be done once (replace drops the table)
        #us = pd.DataFrame(list(users_set))
        #us.to_sql(ListOfUserClustering.__tablename__, session.bind, if_exists='replace')
        for user_id in users_set:
            reg = UserPageRankEvolution(user_id=int(user_id), rank_exec_id=rank_exec_id, rank_step_id=rank_step_id,
                                        rank=rank_df.loc[user_id])
            session.add(reg)
        session.commit()
        
    def get(self):
        Session = sessionmaker(bind=self.engine)
        session = Session()
        q = session.query(UserPageRankEvolution)
        return pd.read_sql(q.statement, session.bind)
        
        