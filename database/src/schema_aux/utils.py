'''
Created on 27 de may. de 2015

@author: lorenzorubio
'''
from os.path import expanduser
import yaml

def get_database_url_sql_alchemy(user=None, alchemy_echo=True):
    '''
    user: alternative user for ddbb connection. If set, overrides connection.properties
    alchemy_echo: if False, disables all messages from sqlalchemy
    '''

    '''
    solve encoding: env variable to be set in the client machine
    controlled here by program - TODO should not overwrite if it is set
    '''
    import os
    os.environ['NLS_LANG'] = 'SPANISH_SPAIN.UTF8'
    
    '''
    TODO export PATH or pass as an argument!
    '''
    properties = yaml.load(open(expanduser("~") + '/.sisifo/connection.properties'))
    database = properties['database']
    if user == None:
        user = database['user']
    if database['dialect'] == 'oracle':
        url = database['dialect'] + "://" + user + ":" + database['password'] + '@' + database['host'] + "/" + database['sid']
    elif database['dialect'] == 'postgresql':
        url = database['dialect'] + "://" + user + ":" + database['password'] + '@' + database['host'] + ":" + database['port'] + "/" + database['database']
    else:
        raise RuntimeError("dialect!")
    return url