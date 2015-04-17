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
        
        