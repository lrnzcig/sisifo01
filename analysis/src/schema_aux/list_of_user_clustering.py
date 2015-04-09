'''
Created on 7 de abr. de 2015

@author: lorenzorubio
'''
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

class DumpTweets():
    '''
    writes tweets into a file from list of users
    '''

    def __init__(self, label, conn):
        '''
        - label: of the cluster
        - conn: oracle connection
        '''
        self.label = label
        self.conn = conn
        
        # export PATH or pass as an argument!
        properties = yaml.load(open(expanduser("~") + '/.sisifo/connection.properties'))
        database = properties['database']
        url = database['dialect'] + "://" + database['user'] + ":" + database['password'] + '@' + database['host'] + "/" + database['sid']
        self.engine = create_engine(url, echo=True)
        Base.metadata.create_all(self.engine)
        return
    
    def dump(self, users_set, additional_label):
        Session = sessionmaker(bind=self.engine)
        session = Session()
        for user in users_set:
            reg = ListOfUserClustering(id=user, cluster_label=self.label, additional_label=additional_label)
            session.add(reg)
        session.commit()
        