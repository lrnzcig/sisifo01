'''
Created on 7 de abr. de 2015

Utilities for clustering

@author: lorenzorubio
'''

user_id_feature_name = 'USER_ID'
screen_name_feature_name = 'SCREEN_NAME'


def get_column_for_follows(user_screen_name, feature_sufix):
    return 'FOLLOWS' + feature_sufix
        
def followers_stats(df, user_screen_name, feature_sufix, summary=True):
    dataframe_column_for_follows = get_column_for_follows(user_screen_name, feature_sufix)
    if (summary == True) & (df.loc[df[dataframe_column_for_follows] != 1, dataframe_column_for_follows].count() == 0):
        return
    print("==> followers of @" + user_screen_name)
    print("number of users following     : " + str(df.loc[df[dataframe_column_for_follows] != 1, dataframe_column_for_follows].count()))
    if (summary == False):
        print("number of users not following : " + str(df.loc[df[dataframe_column_for_follows] == 1, dataframe_column_for_follows].count()))
    
def get_column_for_retweets(user_screen_name, feature_sufix):
    return 'RETWEETS' + feature_sufix
        
def retweets_stats(df, user_screen_name, feature_sufix, summary=True):
    users = get_users_with_retweets(df, user_screen_name, feature_sufix)
    if (summary == True) & (users.count() == 0):
        return
    print("==> retweets of @" + user_screen_name)
    print("number of users with retweets : " + str(users.count()))
    if (summary == False):
        print("number of users w no retweets : " + str(get_users_with_no_retweets(df, user_screen_name, feature_sufix).count()))
    
def get_users_with_retweets(df, user_screen_name, feature_sufix):
    dataframe_column_for_retweets = get_column_for_retweets(user_screen_name, feature_sufix)
    return df.loc[df[dataframe_column_for_retweets] != 1, dataframe_column_for_retweets]
    
def get_users_with_no_retweets(df, user_screen_name, feature_sufix):
    dataframe_column_for_retweets = get_column_for_retweets(user_screen_name, feature_sufix)
    return df.loc[df[dataframe_column_for_retweets] == 1, dataframe_column_for_retweets]
    
def get_column_for_mentions(user_screen_name, feature_sufix):
    return 'MENTIONS' + feature_sufix
        
def mentions_stats(df, user_screen_name, feature_sufix, summary=True):
    dataframe_column_for_mentions = get_column_for_mentions(user_screen_name, feature_sufix)
    if (summary == True) & (df.loc[df[dataframe_column_for_mentions] != 1, dataframe_column_for_mentions].count() == 0):
        return
    print("==> mentions of @" + user_screen_name)
    print("number of users with mentions : " + str(df.loc[df[dataframe_column_for_mentions] != 1, dataframe_column_for_mentions].count()))
    if (summary == False):
        print("number of users w no mentions : " + str(df.loc[df[dataframe_column_for_mentions] == 1, dataframe_column_for_mentions].count()))

def there_are_followers(df, user_screen_name):
    if (get_followers(df, user_screen_name).count() > 0):
        return True
    return False
    
def get_followers(df, user_screen_name):
    feature_sufix = "_" + user_screen_name.upper()
    dataframe_column_for_follows = get_column_for_follows(user_screen_name, feature_sufix)
    return df.loc[df[dataframe_column_for_follows] == 0, screen_name_feature_name]      
    
def find_user_in_df(df, user_screen_name):
    return len(df.loc[df[screen_name_feature_name] == user_screen_name.upper(), screen_name_feature_name])
    

        
