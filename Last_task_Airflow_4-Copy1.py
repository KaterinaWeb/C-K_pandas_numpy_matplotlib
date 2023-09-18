#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from airflow.decorators import dag, task
from airflow.models import Variable


# In[2]:


TOP_1M_DOMAINS = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
TOP_1M_DOMAINS_FILE = 'vgsales.csv'


# In[3]:


default_args = {
    'owner': 'e.juhimenko',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 9, 17),
    'schedule_interval': '0 13 * * *'
}


# In[7]:


@dag(default_args=default_args, catchup=False)
def e_juhimenko_airflow_2():
    
    @task(retries=3)
    def get_data():
        top_data_df = pd.read_csv(TOP_1M_DOMAINS)
        top_data_df.Year= top_data_df.Year.fillna(0)
        top_data_df['Year'] = top_data_df['Year'].astype('int64')
        login = 'e-juhimenko'
        year = 1994 + hash(f'{login}') % 23
        df = top_data_df[top_data_df.Year == year]
        return df
    
    
    @task()
    def global_saled_game(df):
        World_saled_game = df.groupby('Name', as_index = False).agg({'Global_Sales': 'sum'})             .query("Global_Sales == Global_Sales.max()").Name
        return World_saled_game.to_csv(index=False)
    
    
    @task()
    def eu_saled_genre(df):
        EU_saled_genre = df.groupby('Genre', as_index = False).agg({'EU_Sales': 'sum'})             .query('EU_Sales == EU_Sales.max()').Genre
        return EU_saled_genre.to_csv(index=False)
    
    
    @task()
    def na_saled_platform(df):
        gold_Platform_NA = df.groupby(['Platform', 'Name'], as_index = False).agg({'NA_Sales': 'sum'}).             query('NA_Sales > 1.00000').             groupby('Platform', as_index = False).agg({'Name': 'count'}).             query('Name == Name.max()').Platform
        return gold_Platform_NA.to_csv(index=False)
    
    
    @task()
    def jp_saled_publisher(df):
        gold_median_Publisher_JP = df.groupby('Publisher', as_index = False).agg({'JP_Sales': 'median'}).              query('JP_Sales == JP_Sales.max()').Publisher
        return gold_median_Publisher_JP.to_csv(index=False)
    
    
    @task(retries=4, retry_delay=timedelta(3))
    def eu_better_jp(df):
        df_EU = df.groupby('Name', as_index = False).agg({'EU_Sales': 'sum'})
        df_JP = df.groupby('Name', as_index = False).agg({'JP_Sales': 'sum'})
        df_EU_JP = pd.merge(df_EU,df_JP, how = 'left')
        df_EU_JP['EU_winner'] = df_EU_JP.EU_Sales > df_EU_JP.JP_Sales
        EU_better_JP_times = df_EU_JP[df_EU_JP.EU_winner == True].count().Name
        return EU_better_JP_times
    
    
    @task(retries=4, retry_delay=timedelta(4))
    def print_data(World_saled_game, EU_saled_genre, gold_Platform_NA,gold_median_Publisher_JP, EU_better_JP_times):
        login = 'e-juhimenko'
        year = 1994 + hash(f'{login}') % 23
        
        print(f'1. Самая продаваемая игра в {year} году во всем мире')
        print(World_saled_game)
        
        print(f'2. Самый(-ые) продаваемый(-ые) в Европе жанр(-ы) в {year} году')
        print(EU_saled_genre)
        
        print(f'3. Больше всего игр, которые продались более чем миллионным тиражом в Северной Америке в {year} году были на платформе')
        print(gold_Platform_NA)
        
        print(f'4. Самые высокие средние продажи в Японии в {year} году были у издательства')
        print(gold_median_Publisher_JP)
        
        print(f'5. Количество игр, которые продались лучше в Европе, чем в Японии в {year} году составляет {EU_better_JP_times}')

        

    df = get_data()
    World_saled_game = global_saled_game(df)
    EU_saled_genre = eu_saled_genre(df)
    gold_Platform_NA = na_saled_platform(df)
    gold_median_Publisher_JP = jp_saled_publisher(df)
    EU_better_JP_times = eu_better_jp(df)
    
    print_data(World_saled_game, EU_saled_genre, gold_Platform_NA,gold_median_Publisher_JP, EU_better_JP_times)

e_juhimenko_airflow_2 = e_juhimenko_airflow_2()
  


# In[129]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




