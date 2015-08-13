'''
Created on 1 de abr. de 2015

@author: lorenzorubio
'''
import cx_Oracle

class Manager():
    '''
    Manages twitter tables
    '''


    def __init__(self, conn):
        '''
        Constructor
        '''
        self.sconnection = conn
    
    def delete_all(self):
        cur = self.sconnection.get().cursor()
        try:
            cur.execute("delete from tweet")
            cur.execute("delete from tuser")
            cur.execute("delete from thashtag")
            cur.execute("delete from turl")
            cur.execute("delete from tusermention")
            cur.execute("delete from tuserurl")
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            self.sconnection.get().rollback()
            raise RuntimeError(error)
        else:
            self.sconnection.get().commit()
            return


        