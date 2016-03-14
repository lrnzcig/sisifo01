'''
Created on 22 de sept. de 2015

@author: lorenzorubio
'''
import pandas as pd
from sqlalchemy import Column, String, Float, BigInteger, Integer, ForeignKey, DateTime, Sequence, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import schema_aux.utils as utils

Base = declarative_base()

class UserRankExec(Base):
    __tablename__ = 'user_rank_exec'
    '''
    autoincrement=True works fine for postgres, without the Sequence
    (ends up creating a sequence with different parameters)
    the Sequence is needed in oracle, and works for both
    '''
    id = Column(Integer, Sequence('user_rank_exec_seq'), primary_key=True)
    rank_exec_label = Column(String(256))
    hour_step = Column(Integer)
    user_type = Column(String(50))
    
    def __repr__(self):
        return "<UserRankExecution(id='%s', rank_exec_label='%s')>" % (
            self.user_id, self.rank_exec_label)

class UserRankExecStep(Base):
    __tablename__ = 'user_rank_exec_step'
    '''
    autoincrement=True works fine for postgres, without the Sequence
    (ends up creating a sequence with different parameters)
    the Sequence is needed in oracle, and works for both
    '''
    id = Column(Integer, Sequence('user_rank_exec_step_seq'), primary_key=True)
    rank_exec_id = Column(Integer, ForeignKey('user_rank_exec.id'))
    rank_step_label = Column(String(256))
    step_order = Column(Integer)
    step_timestamp = Column(DateTime)
    
    def __repr__(self):
        return "<UserRankExecStep(id='%s', rank_step_label='%s')>" % (
            self.user_id, self.rank_step_label)

class UserRankEvolution(Base):
    __tablename__ = 'user_rank_evolution'
    user_id = Column(BigInteger, primary_key=True)
    rank_exec_id = Column(Integer, ForeignKey('user_rank_exec.id'), primary_key=True)
    step_order = Column(Integer, primary_key=True)
    rank = Column(Float)
    
    def __repr__(self):
        return "<UserRankEvolution(user_id='%s', rank_exec_id='%s', order='%s')>" % (
            self.user_id, self.rank_exec_id, self.step_order)

class Manager():
    '''
    manager for list of users by rank
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
        session.query(UserRankEvolution).delete()
        session.commit()
    
    def dump(self, users_set, rank_exec_label, rank_step_label, rank_df, order, hours_step, step_timestamp,
             user_type="twitter"):
        Session = sessionmaker(bind=self.engine)
        session = Session()
        # NOTE: this could only be done once (replace drops the table)
        #us = pd.DataFrame(list(users_set))
        #us.to_sql(ListOfUserClustering.__tablename__, session.bind, if_exists='replace')
        exec_reg = session.query(UserRankExec) \
                   .filter(UserRankExec.rank_exec_label == rank_exec_label) \
                   .first()
        if not exec_reg:
            exec_reg = UserRankExec(rank_exec_label=rank_exec_label,
                                    hour_step=hours_step,
                                    user_type=user_type)
            session.add(exec_reg)
            
            # TODO fix sequences read-back value
            session.commit()
            exec_reg = session.query(UserRankExec) \
                       .filter(UserRankExec.rank_exec_label == rank_exec_label) \
                       .first()
            
        exec_step_reg = UserRankExecStep(rank_exec_id=exec_reg.id,
                                         rank_step_label=rank_step_label,
                                         step_order=order,
                                         step_timestamp=step_timestamp)
        session.add(exec_step_reg)        
        
        for user_id in users_set:
            reg = UserRankEvolution(user_id=int(user_id), rank_exec_id=exec_reg.id,
                                    rank=float(rank_df.loc[user_id]), step_order=order)
            session.add(reg)
        session.commit()
        
    def get(self):
        Session = sessionmaker(bind=self.engine)
        session = Session()
        q = session.query(UserRankEvolution)
        return pd.read_sql(q.statement, session.bind)
        
        