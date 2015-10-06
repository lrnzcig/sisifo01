'''
Created on 22 de sept. de 2015

@author: lorenzorubio
'''
import pandas as pd
from sqlalchemy import Column, String, Float, BigInteger, Integer, ForeignKey, DateTime, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import schema_aux.utils as utils

Base = declarative_base()

class UserPageRankExec(Base):
    __tablename__ = 'user_page_rank_exec'
    id = Column(Integer, primary_key=True, autoincrement=True)
    rank_exec_label = Column(String(256))
    hour_step = Column(Integer)
    
    def __repr__(self):
        return "<UserPageRankExecution(id='%s', rank_exec_label='%s')>" % (
            self.user_id, self.rank_exec_label)

class UserPageRankExecStep(Base):
    __tablename__ = 'user_page_rank_exec_step'
    id = Column(Integer, primary_key=True, autoincrement=True)
    rank_exec_id = Column(Integer, ForeignKey('user_page_rank_exec.id'))
    rank_step_label = Column(String(256))
    step_order = Column(Integer)
    step_timestamp = Column(DateTime)
    
    def __repr__(self):
        return "<UserPageRankExecStep(id='%s', rank_step_label='%s')>" % (
            self.user_id, self.rank_step_label)

class UserPageRankEvolution(Base):
    __tablename__ = 'user_page_rank_evolution'
    user_id = Column(BigInteger, primary_key=True)
    rank_exec_id = Column(Integer, ForeignKey('user_page_rank_exec.id'), primary_key=True)
    step_order = Column(Integer, primary_key=True)
    rank = Column(Float)
    
    def __repr__(self):
        return "<UserPageRankEvolution(user_id='%s', rank_exec_id='%s', order='%s')>" % (
            self.user_id, self.rank_exec_id, self.step_order)

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
    
    def dump(self, users_set, rank_exec_label, rank_step_label, rank_df, order, hours_step, step_timestamp):
        Session = sessionmaker(bind=self.engine)
        session = Session()
        # NOTE: this could only be done once (replace drops the table)
        #us = pd.DataFrame(list(users_set))
        #us.to_sql(ListOfUserClustering.__tablename__, session.bind, if_exists='replace')
        exec_reg = session.query(UserPageRankExec) \
                   .filter(UserPageRankExec.rank_exec_label == rank_exec_label) \
                   .first()
        if not exec_reg:
            exec_reg = UserPageRankExec(rank_exec_label=rank_exec_label,
                                        hours_step=hours_step)
            session.add(exec_reg)
            
        exec_step_reg = UserPageRankExecStep(rank_exec_id=exec_reg.id,
                                             rank_step_label=rank_step_label,
                                             step_order=order,
                                             step_timestamp=step_timestamp)
        session.add(exec_step_reg)
        
        for user_id in users_set:
            reg = UserPageRankEvolution(user_id=int(user_id), rank_exec_id=exec_reg.id,
                                        rank=float(rank_df.loc[user_id]), step_order=order)
            session.add(reg)
        session.commit()
        
    def get(self):
        Session = sessionmaker(bind=self.engine)
        session = Session()
        q = session.query(UserPageRankEvolution)
        return pd.read_sql(q.statement, session.bind)
        
        