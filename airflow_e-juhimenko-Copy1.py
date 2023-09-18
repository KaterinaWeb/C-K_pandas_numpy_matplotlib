#!/usr/bin/env python
# coding: utf-8

# In[106]:


import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


# In[107]:


TOP_1M_DOMAINS = 'https://storage.yandexcloud.net/kc-startda/top-1m.csv'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


# In[109]:


def get_data():
    
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def top_10_zone():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['zone'] = top_data_df['domain'].str.split('.').str[-1]                                                      
    top_10_domain_zone = top_data_df.zone.value_counts().to_frame().reset_index()         .rename(columns = {'zone':'count', 'index':'domain_zone'}).head(10).domain_zone                                                      
   
    with open('top_10_domain_zone.csv', 'w') as f:
        f.write(top_10_domain_zone.to_csv(index=False))


def longest_dom():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['name'] = top_data_df['domain'].str.split('.').str[0]            
    top_data_df['lenght_name'] = top_data_df.name.apply(lambda x: len(x))
    longest_domain = top_data_df.sort_values(['lenght_name', 'name'], ascending = [False, True]).head(1).domain            
    
    with open('longest_domain.csv', 'w') as f:
        f.write(longest_domain.to_csv(index=False))
                
                
def rank_airflow():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    try:
        airflow_rank = top_data_df[top_data_df.domain == 'airflow.com']['rank']
    except:
        airflow_rank = 'Домена airflow.com в рейтинге не обнаружено!'            
    
    with open('airflow_rank.csv', 'w') as f:
        f.write(airflow_rank.to_csv(index=False))                


def print_data(ds):
    with open('top_10_domain_zone.csv', 'r') as f:
        top_zone_data = f.read()
    with open('longest_domain.csv', 'r') as f:
        longest_dom_data = f.read()
    with open('airflow_rank.csv', 'r') as f:
        airflow_rank_data = f.read()
    date = ds

    print(f'Топ-10 доменных зон по численности доменов по состоянию на {date}')
    print(top_zone_data)

    print(f'Домен с самым длинным именем по состоянию на {date}')
    print(longest_dom_data)
                
    print(f'Домен airflow.com по состоянию на {date} находится на месте')
    print(airflow_rank_data)


# In[110]:


default_args = {
    'owner': 'e.juhimenko',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 9, 15),
    'schedule_interval': '0 15 * * *'
}
dag = DAG('e-juhimenko_domain', default_args=default_args)


# In[111]:


t1 = PythonOperator(task_id='get_data_juhimenko',
                    python_callable=get_data,
                    dag=dag)

t2 = PythonOperator(task_id='top_10_zone_juhimenko',
                    python_callable=top_10_zone,
                    dag=dag)

t2_longest = PythonOperator(task_id='longest_dom_juhimenko',
                        python_callable=longest_dom,
                        dag=dag)

t2_airflow = PythonOperator(task_id='rank_airflow_juhimenko',
                        python_callable=rank_airflow,
                        dag=dag)


t3 = PythonOperator(task_id='print_data_juhimenko',
                    python_callable=print_data,
                    dag=dag)


# In[112]:


t1 >> [t2, t2_longest, t2_airflow] >> t3


# In[ ]:





# In[ ]:




