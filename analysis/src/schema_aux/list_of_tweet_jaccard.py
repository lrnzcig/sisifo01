'''
Created on 7 de abr. de 2015

@author: lorenzorubio
'''
import pandas as pd
from sqlalchemy import Column, Integer, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import schema_aux.utils as utils

Base = declarative_base()

class ListOfTweetJaccardClass(Base):
    __tablename__ = 'clases_equi_70'
    id1 = Column(Integer, primary_key=True)
    clase_equi = Column(Integer)
    num_tuits = Column(Integer)
    
    def __repr__(self):
        return "<ListOfTweetJaccardClass(id='%s'')>" % (
            self.id1)

class Manager():
    '''
    writes & reads list of users
    '''
    def __init__(self, user=None, alchemy_echo=True, delete_all=False):
        url = utils.get_database_url_sql_alchemy(user, alchemy_echo)
        self.engine = create_engine(url, echo=False)
        Base.metadata.create_all(self.engine)
        if (delete_all == True):
            Session = sessionmaker(bind=self.engine)
            session = Session()
            session.query(ListOfTweetJaccardClass).delete()
            session.commit()
    
    def dump_classes(self, df):
        Session = sessionmaker(bind=self.engine)
        session = Session()
        for item in df.iterrows():
            reg = ListOfTweetJaccardClass(proxy_id=item[0], number_of_tweets=item[1]['number_of_tweets'])
            session.add(reg)
        session.commit()
        

    def get_classes(self):
        Session = sessionmaker(bind=self.engine)
        session = Session()
        q = session.query(ListOfTweetJaccardClass)
        return pd.read_sql(q.statement, session.bind)
        
        