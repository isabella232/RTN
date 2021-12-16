#!/usr/bin/env python
# coding: utf-8

# In[ ]


#!/usr/bin/env python
# coding: utf-8


#############################################################
# Libraries
#############################################################
from IPython import get_ipython
from covid19dh import covid19
import pytz
#import tweepy as tw
import pandas as pd
import numpy as np
from teradataml.dataframe.dataframe import DataFrame, in_schema
from teradataml.dataframe.copy_to import copy_to_sql
from teradataml.context.context import create_context, remove_context, get_connection, get_context
from teradataml.options.display import display
from teradataml.dataframe.fastload import fastload
#import tdconnect
import getpass
import datetime
from datetime import date
from datetime import datetime, timedelta
from datetime import datetime
from teradataml.context.context import *
import ssl
ssl._create_default_https_context = ssl._create_unverified_context
import params
con = create_context(host=params.MyHost, username=params.MyUser, password=params.Password, temp_database_name=params.SchemaName, logmech=params.LogMech)


#############################################################
# 1) Apple Mobility
#############################################################
#import datetime

#url = 'https://covid19-static.cdn-apple.com/covid19-mobility-data/2010HotfixDev23/v3/en-us/applemobilitytrends-2020-06-18.csv'
#df = pd.read_csv(url)


#df['MergedColumn'] = df[df.columns[0:]].apply(
#    lambda x: '|'.join(x.dropna().astype(str)),
#    axis=1
#)


#df.drop(df.columns.difference(['MergedColumn']), 1, inplace=True)
#df['current_dttm'] = datetime.datetime.today()

#fastload(df = df, table_name = "STG_Apple_Mobility", schema_name=params.SchemaName, if_exists = 'replace')

#from datetime import datetime
#datetime.utcnow()
#dateTimeObj = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone('US/Pacific'))
#timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
#print("Apple Mobility Finished!  " + timestampStr)


#############################################################
# 2) Covid Cases
#############################################################
from datetime import datetime
import datetime

url = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv'
df = pd.read_csv(url)
df['current_dttm'] = datetime.datetime.today()
df = df.rename(columns={'date': 'date_key'})
df = df.loc[df['date_key'] >= '2021-01-01']

df['date_key'] = pd.to_datetime(df['date_key'])
# ## Added for NOS purpose
# df['year'], df['month'] = df['date_key'].dt.year, df['date_key'].dt.month

copy_to_sql(df = df, table_name = "STG_covid19_stats", schema_name=params.SchemaName , primary_index = ['date_key'], if_exists = 'replace')

from datetime import datetime
datetime.utcnow()
dateTimeObj = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone('US/Pacific'))
timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
print("Covid Cases Finished!  " + timestampStr)


#############################################################
# 3) Covid Projections
#############################################################
from datetime import datetime
import datetime

# Link for new format of data
url = 'https://ihmecovid19storage.blob.core.windows.net/latest/data_download_file_reference_2021.csv'
df = pd.read_csv(url)

df.rename(columns={'hospital_beds_lower': 'allbed_lower', 'hospital_beds_mean': 'allbed_mean', 
                   'hospital_beds_upper': 'allbed_upper', 'daily_infections': 'confirmed_infections',
                   'seir_daily_unscaled_lower': 'deaths_lower', 'seir_daily_unscaled_mean': 'deaths_mean',
                   'seir_daily_unscaled_upper': 'deaths_upper', 'seir_daily_lower': 'deaths_lower_smoothed', 
                   'seir_daily_mean': 'deaths_mean_smoothed', 'seir_daily_upper': 'deaths_upper_smoothed',
                   'inf_lower': 'est_infections_lower', 'inf_mean': 'est_infections_mean', 'inf_upper': 'est_infections_upper',
                   'icu_beds_lower': 'ICUbed_lower', 'icu_beds_mean': 'ICUbed_mean', 'icu_beds_upper': 'ICUbed_upper',
                   'mobility_mean': 'mobility_composite', 'mobility_obs': 'mobility_data_type',
                   'population': 'total_pop', 'testing_mean': 'total_tests', 'testing_obs': 'total_tests_data_type',
                   'seir_cumulative_unscaled_lower': 'totdea_lower', 'seir_cumulative_unscaled_mean': 'totdea_mean',
                   'seir_cumulative_unscaled_upper': 'totdea_upper', 'seir_cumulative_lower': 'totdea_lower_smoothed',
                   'seir_cumulative_mean': 'totdea_mean_smoothed', 'seir_cumulative_upper': 'totdea_upper_smoothed'}, inplace=True)

# Reference Hospitalization Data
df_hosp = df.drop(['all_bed_capacity','icu_bed_capacity'], axis=1)
df_hosp_sorted = df_hosp.sort_values(by = ['location_name'], ascending=True)
df_hosp_sorted.reset_index(drop=True, inplace=True)

df_hosp_sorted.insert(loc=0, column='V1', value=df_hosp.index + 1)
df_hosp_sorted['Path_Update_Dt'] = 'reference_hospitalization_all_locs.csv'
df_hosp_sorted['current_dttm'] = datetime.datetime.today()

# Summary Stats Data (by downsizing data)
df_summ = df[['location_id','location_name','all_bed_capacity','icu_bed_capacity']].drop_duplicates()
df_summ_sorted = df_summ.sort_values(by = ['location_name'], ascending=True)
df_summ_sorted.reset_index(drop=True, inplace=True)

df_summ_sorted['Path_Update_Dt'] = 'Summary_stats_all_locs.csv'
df_summ_sorted['current_dttm'] = datetime.datetime.today()


copy_to_sql(df = df_hosp_sorted, table_name = 'STG_Hospitalization_all_locs', schema_name=params.SchemaName, index=False, if_exists="replace")
copy_to_sql(df = df_summ_sorted, table_name = 'STG_Summary_stats_all_locs', schema_name=params.SchemaName, index=False, if_exists="replace")

from datetime import datetime        
datetime.utcnow()
dateTimeObj = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone('US/Pacific'))
timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
print("Covid Projections Finished!  " + timestampStr)


#############################################################
# 4)  Google Trends
#############################################################
import pytrends
from pytrends.request import TrendReq
import datetime

strtdt = datetime.date(2020, 1, 1)
enddt = date.today()
timeframe=strtdt.strftime("%Y-%m-%d")+' '+enddt.strftime("%Y-%m-%d") 

pytrends = TrendReq(hl='en-US', tz=360)


a='covid + coronavirus'
cat=''
kw_list = [a]
pytrends.build_payload(kw_list, timeframe=timeframe,geo='US') 
iot=pytrends.interest_over_time()
iot = iot.rename(columns={a: 'Metric_value'})
iot['Trend_Name']='Covid Search'
iot['Metric_Name']='Covid'
iot['current_dttm'] = datetime.datetime.today()
iot['Keyword_List'] =a
iot['Cat_CD']=cat
iot['Type'] ='Interest over time'

iot['isPartial'] = iot['isPartial'].astype(str).str.replace('True','1')
iot['isPartial'] = iot['isPartial'].astype(str).str.replace('False','0')

iot.reset_index(inplace=True)
# ## Added for NOS purpose
# iot['TheYear'], iot['TheMonth'] = iot['date'].dt.year, iot['date'].dt.month

copy_to_sql(df = iot, table_name = "STG_Google_Search_IOT", schema_name=params.SchemaName, index=False, if_exists="replace") 


a=''
cat='1085'
kw_list = [a]
pytrends.build_payload(kw_list, cat=cat, timeframe=timeframe,geo='US')
iot=pytrends.interest_over_time()
iot = iot.rename(columns={a: 'Metric_value'})
iot['Trend_Name']='Movie Listings & Theater Showtimes'
iot['Metric_Name']='Interest over time'
iot['current_dttm'] = datetime.datetime.today()
iot['Keyword_List'] =a
iot['Cat_CD']=cat
iot['Type'] ='Interest over time'

iot['isPartial'] = iot['isPartial'].astype(str).str.replace('True','1')
iot['isPartial'] = iot['isPartial'].astype(str).str.replace('False','0')

iot.reset_index(inplace=True)
# ## Added for NOS purpose
# iot['TheYear'], iot['TheMonth'] = iot['date'].dt.year, iot['date'].dt.month

copy_to_sql(df = iot, table_name = "STG_Google_Search_IOT", schema_name=params.SchemaName, index=False, if_exists="append") 


a=''
cat='208'
kw_list = [a]
pytrends.build_payload(kw_list, cat=cat, timeframe=timeframe,geo='US')
iot=pytrends.interest_over_time()
iot['Trend_Name']='Tourist Destinations'
iot = iot.rename(columns={a: 'Metric_value'})
iot['Metric_Name']='Interest over time'
iot['current_dttm'] = datetime.datetime.today()
iot['Keyword_List'] =a
iot['Cat_CD']=cat
iot['Type'] ='Interest over time'

iot['isPartial'] = iot['isPartial'].astype(str).str.replace('True','1')
iot['isPartial'] = iot['isPartial'].astype(str).str.replace('False','0')

iot.reset_index(inplace=True)
# ## Added for NOS purpose
# iot['TheYear'], iot['TheMonth'] = iot['date'].dt.year, iot['date'].dt.month

copy_to_sql(df = iot, table_name = "STG_Google_Search_IOT", schema_name=params.SchemaName, index=False, if_exists="append") 


from datetime import datetime
datetime.utcnow()
dateTimeObj = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone('US/Pacific'))
timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
print("Google Trends Finished!  " + timestampStr)


#############################################################
# 5)  Google Mobility
#############################################################
import datetime

url = 'https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv'
df = pd.read_csv(url, dtype='unicode')

df['current_dttm'] = datetime.datetime.today()
df = df.rename(columns={'date': 'date_key'})

df = df.loc[df['date_key'] >= '2021-01-01']

copy_to_sql(df = df, table_name = "STG_Google_Mobility", schema_name=params.SchemaName, primary_index = ['date_key'], if_exists = 'replace')

from datetime import datetime
datetime.utcnow()
dateTimeObj = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone('US/Pacific'))
timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
print("Google Mobility Finished!  " + timestampStr)


#############################################################
# 6)  COVID Datahub Level 3
#############################################################
import datetime

df, src = covid19("USA", level = 3, start = date(2021,1,1), verbose = False)
df['current_dttm'] = datetime.datetime.today()
df = df.rename(columns={'date': 'date_key'})

# ## Added for NOS purpose
# df['month'], df['day'] = df['date_key'].dt.month, df['date_key'].dt.day

copy_to_sql(df = df, table_name = "STG_COVID19_Datahub_LVL3", schema_name=params.SchemaName, primary_index = ['date_key'], if_exists = 'replace')

from datetime import datetime
datetime.utcnow()
dateTimeObj = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone('US/Pacific'))
timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
print("COVID Datahub Level 3 Finished!  " + timestampStr)


#############################################################
# 7)  COVID Datahub Level 2
#############################################################
import datetime

df, src = covid19("USA", level = 2, start = date(2021,1,1), verbose = False)
df['current_dttm'] = datetime.datetime.today()
df = df.rename(columns={'date': 'date_key'})

copy_to_sql(df = df, table_name = "STG_COVID19_Datahub_LVL2", schema_name=params.SchemaName, primary_index = ['date_key'], if_exists = 'replace')

from datetime import datetime
datetime.utcnow()
dateTimeObj = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone('US/Pacific'))
timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
print("COVID Datahub Level 2 Finished!  " + timestampStr)


#############################################################
# 8)  Labor Stats Data
#############################################################
import json
import requests
import datetime

#data = json.dumps({"seriesid": ['CUSR0000SA0','SUUR0000SA0'],"startyear":"2019", "endyear":"2020"})
headers = {'Content-type': 'application/json'}
data = json.dumps({"seriesid": ['CUSR0000SA0','LNS13000000','LNS14000000'],"startyear":"2018", "endyear":"2021"})
p = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data, headers=headers, verify=False)
json_data = json.loads(p.text)

df0 = pd.DataFrame(json_data['Results']['series'][0]['data'])
df0 = df0.drop(['footnotes'], axis=1)
df0.loc[:,'footnotes']="Consumer Price Index"
df0.loc[:,'series_id']="CUSR0000SA0"
df0['current_dttm'] = datetime.datetime.today()
df0.rename(columns={'year': 'Year_Key', 'periodName': 'Period_Month', 'value': 'Metric_Val', 'period': 'Period_Key'}, inplace=True)

if "calculations" in df0:
  del df0["calculations"]


copy_to_sql(df = df0, table_name = "STG_Labor_Stats_CUSR0000SA0", schema_name = params.SchemaName, index=False, if_exists="replace")


df1 = pd.DataFrame(json_data['Results']['series'][1]['data'])
df1 = df1.drop(['footnotes'], axis=1)
df1.loc[:,'footnotes']="Unemployment Level"
df1.loc[:,'series_id']="LNS13000000"
df1['current_dttm'] = datetime.datetime.today()
df1.rename(columns={'year': 'Year_Key', 'periodName': 'Period_Month', 'value': 'Metric_Val', 'period': 'Period_Key'}, inplace=True)

if "calculations" in df1:
  del df1["calculations"]


copy_to_sql(df = df1, table_name = "STG_Labor_Stats_LNS13000000", schema_name = params.SchemaName, index=False, if_exists="replace")


df2 = pd.DataFrame(json_data['Results']['series'][2]['data'])
df2 = df2.drop(['footnotes'], axis=1)
df2.loc[:,'footnotes']="Unemployment Rate"
df2.loc[:,'series_id']="LNS14000000"
df2['current_dttm'] = datetime.datetime.today()
df2.rename(columns={'year': 'Year_Key', 'periodName': 'Period_Month', 'value': 'Metric_Val', 'period': 'Period_Key'}, inplace=True)

if "calculations" in df2:
  del df2["calculations"]


copy_to_sql(df = df2, table_name = "STG_Labor_Stats_LNS14000000", schema_name = params.SchemaName, index=False, if_exists="replace")

from datetime import datetime
datetime.utcnow()
dateTimeObj = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone('US/Pacific'))
timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
print("Labor Stats Finished!  " + timestampStr)


#############################################################
# 9) Fuel Production
#############################################################
import datetime

url = 'https://www.eia.gov/dnav/pet/xls/PET_CONS_WPSUP_K_4.xls'
df = pd.read_excel(url, sheet_name = "Data 1", skiprows=[0,1])
df['current_dttm'] = datetime.datetime.today()
df = df.rename(columns={'Date': 'date_key'})

# ## Added for NOS purpose
# df['year'], df['month'] = df['date_key'].dt.year, df['date_key'].dt.month

copy_to_sql(df = df, table_name = "STG_Fuel_Production", schema_name = params.SchemaName, if_exists = 'replace')

from datetime import datetime
datetime.utcnow()
dateTimeObj = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone('US/Pacific'))
timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
print("Fuel Production Finished!  " + timestampStr)


#############################################################
# 10) TSA Travel
#############################################################
import requests
from bs4 import BeautifulSoup
from requests_html import HTMLSession
import datetime

session = HTMLSession()
url = 'https://www.tsa.gov/coronavirus/passenger-throughput'
r = session.get(url)
soup=BeautifulSoup(r.html.html,'html.parser')
stat_table = soup.find('table')

df = pd.read_html(str(stat_table),header=0)[0]

df.columns = ('Travel_Date',
              'TravelThroughPut', 
              'TravelThroughPutLastYear',
              'TravelThroughPut2019')

df['current_dttm'] = datetime.datetime.today()

df['Travel_Date'] = pd.to_datetime(df['Travel_Date'])
# ## Added for NOS purpose
# df['year'], df['month'] = df['Travel_Date'].dt.year, df['Travel_Date'].dt.month

copy_to_sql(df = df, table_name = "STG_TSA_TRAVEL", schema_name = params.SchemaName, if_exists = 'replace')

from datetime import datetime
datetime.utcnow()
dateTimeObj = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone('US/Pacific'))
timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
print("TSA Travel Finished!  " + timestampStr)


#############################################################
# 11) CENSUS Data
#############################################################
import datetime

url = 'https://www.census.gov/econ/currentdata/export/csv?programCode=RESCONST&timeSlotType=12&startYear=2018&endYear=2021&categoryCode=APERMITS&dataTypeCode=TOTAL&geoLevelCode=US&adjusted=yes&errorData=no&internal=false'
hd = pd.read_csv(url, sep='~',  header=None, nrows=6, keep_default_na=False)
desc=''
for (idx, row) in hd.iterrows():
	desc=desc+row.loc[0]+'\n' 

df = pd.read_csv(url, skiprows=6, keep_default_na=False, dtype={'Value': str})
df['Metric_name']='Housing Starts in 1000s'
df['Data_source_desc']=desc
df['current_dttm'] = datetime.datetime.today()

# ## Added for NOS purpose
# df['date_key'] = pd.to_datetime(df['Period'])
# df['year'] = df['date_key'].dt.year

copy_to_sql(df = df, table_name = "STG_US_CENSUS_SURVEY", schema_name=params.SchemaName, primary_index = ['Period'], if_exists = 'replace')

from datetime import datetime
datetime.utcnow()
dateTimeObj = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone('US/Pacific'))
timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
print("Census Data Finished!  " + timestampStr)


#############################################################
# 12) Consumer Sentiment Index
#############################################################
from io import BytesIO
from urllib.parse import quote_plus
import datetime

url = 'http://www.sca.isr.umich.edu/files/tbmics.csv'
res = requests.get(url)
df = pd.read_csv(BytesIO(res.content), dtype = {'YYYY':str})
df = df.dropna()
df.columns = ('Month','Year','Consumer_Sentiment_Index')
df = df.loc[df['Year'] >= '2020']

df['current_dttm'] = datetime.datetime.today()

copy_to_sql(df = df, table_name = "STG_Consumer_Sentiment_Index", schema_name=params.SchemaName, if_exists = 'replace')

from datetime import datetime
datetime.utcnow()
dateTimeObj = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone('US/Pacific'))
timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
print("Consumer Sentiment Index Finished!  " + timestampStr)


#############################################################
# 13) Estimated Hospitalization
#############################################################
import json
import requests
import datetime

url = 'https://healthdata.gov/resource/jjp9-htie.csv?$limit=10000'
df = pd.read_csv(url, dtype='unicode')
df.reset_index(drop=True, inplace=True)
df['current_dttm'] = datetime.datetime.today()
df = df.drop(['geocoded_state'], axis = 1)
df['collection_date'] = pd.to_datetime(df['collection_date']).dt.date
df.rename(columns={'inpatient_beds_occupied': 'Inpatient Beds Occupied Estimated', 'count_ll': 'Count LL', 'count_ul': 'Count UL'
,'percentage_of_inpatient_beds': 'Percentage of Inpatient Beds Occupied Estimated', 'percentage_ll': 'Percentage LL', 'percentage_ul': 'Percentage UL'
,'total_inpatient_beds': 'Total Inpatient Beds', 'total_ll': 'Total LL', 'total_ul': 'Total UL'
}, inplace=True)

copy_to_sql(df = df, table_name = "STG_Estimated_Inpatient_All", schema_name=params.SchemaName, if_exists = 'replace')


url = 'https://healthdata.gov/resource/py8k-j5rq.csv?$limit=10000'
df = pd.read_csv(url, dtype='unicode')
df.reset_index(drop=True, inplace=True)
df['current_dttm'] = datetime.datetime.today()
df['collection_date'] = pd.to_datetime(df['collection_date']).dt.date
df = df.drop(['geocoded_state'], axis = 1)
df.rename(columns={'inpatient_beds_occupied_by': 'Inpatient Beds Occupied by COVID-19 Patients Estimated', 'count_ll': 'Count LL', 'count_ul': 'Count UL'
,'percentage_of_inpatient_beds': 'Percentage of Inpatient Beds Occupied by COVID-19 Patients Estimated', 'percentage_ll': 'Percentage LL', 'percentage_ul': 'Percentage UL'
,'total_inpatient_beds': 'Total Inpatient Beds', 'total_ll': 'Total LL', 'total_ul': 'Total UL'
}, inplace=True)

copy_to_sql(df = df, table_name = "STG_Estimated_Inpatient_Covid", schema_name=params.SchemaName, if_exists = 'replace')


url = 'https://healthdata.gov/resource/7ctx-gtb7.csv?$limit=10000'
df = pd.read_csv(url, dtype='unicode')
df.reset_index(drop=True, inplace=True)
df['current_dttm'] = datetime.datetime.today()
df['collection_date'] = pd.to_datetime(df['collection_date']).dt.date
df = df.drop(['geocoded_state'], axis = 1)
df.rename(columns={'staffed_adult_icu_beds_occupied_est': 'Staffed Adult ICU Beds Occupied Estimated', 'count_ll': 'Count LL', 'count_ul': 'Count UL'
,'percentage_of_staffed_adult_icu_beds_occupied_est': 'Percentage of Staffed Adult ICU Beds Occupied Estimated', 'percentage_ll': 'Percentage LL', 'percentage_ul': 'Percentage UL'
,'total_staffed_adult_icu_beds': 'Total Staffed Adult ICU Beds', 'total_ll': 'Total LL', 'total_ul': 'Total UL'
}, inplace=True)

copy_to_sql(df = df, table_name = "STG_Estimated_Icu", schema_name=params.SchemaName, if_exists = 'replace')


from datetime import datetime
datetime.utcnow()
dateTimeObj = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone('US/Pacific'))
timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
print("Estimated Hospitalization Finished!  " + timestampStr)


#############################################################
#14) Loading time series data for Vaccinations
#############################################################
url = 'https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/vaccinations/us_state_vaccinations.csv'
df = pd.read_csv(url)

copy_to_sql(df, table_name="US_STATE_VAC", schema_name=params.SchemaName, if_exists="replace")

con.execute ("UPDATE "+params.SchemaName+".US_STATE_VAC SET location = 'New York' WHERE location = 'New York State';")

from datetime import datetime
datetime.utcnow()
dateTimeObj = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone('US/Pacific'))
timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
print("Time Series Data for Vaccination Finished!  " + timestampStr)


#############################################################
# Printing the Load Summary Stats
#############################################################
remove_context()
con = create_context(host=params.MyHost, username=params.MyUser, password=params.Password,temp_database_name=params.SchemaName,logmech=params.LogMech)

pda = pd.read_sql('DATABASE '+params.SchemaName,con)
query = "SELECT 'Python' as Process_Name, 'Staging' as Table_Type, 'STG_Hospitalization_all_locs' as TableName, count(*) as Records_Processed, max(current_dttm) as Process_Dttm FROM STG_Hospitalization_all_locs GROUP BY 1,2,3 \
UNION \
SELECT 'Python' as Process_Name, 'Staging' as Table_Type, 'STG_Summary_stats_all_locs' as TableName, count(*) as Records_Processed, max(current_dttm) as Process_Dttm FROM STG_Summary_stats_all_locs GROUP BY 1,2,3 \
UNION \
SELECT 'Python' as Process_Name, 'Staging' as Table_Type, 'STG_covid19_stats' as TableName, count(*) as Records_Processed, max(current_dttm) as Process_Dttm FROM STG_covid19_stats GROUP BY 1,2,3 \
UNION \
SELECT 'Python' as Process_Name, 'Staging' as Table_Type, 'STG_Google_Search_IOT' as TableName, count(*) as Records_Processed, max(current_dttm) as Process_Dttm FROM STG_Google_Search_IOT GROUP BY 1,2,3 \
UNION \
SELECT 'Python' as Process_Name, 'Staging' as Table_Type, 'STG_Google_Mobility' as TableName, count(*) as Records_Processed, max(current_dttm) as Process_Dttm FROM STG_Google_Mobility GROUP BY 1,2,3 \
UNION \
SELECT 'Python' as Process_Name, 'Staging' as Table_Type, 'STG_Labor_Stats_LNS13000000' as TableName, count(*) as Records_Processed, max(current_dttm) as Process_Dttm FROM STG_Labor_Stats_LNS13000000 GROUP BY 1,2,3 \
UNION \
SELECT 'Python' as Process_Name, 'Staging' as Table_Type, 'STG_Labor_Stats_LNS14000000' as TableName, count(*) as Records_Processed, max(current_dttm) as Process_Dttm FROM STG_Labor_Stats_LNS14000000 GROUP BY 1,2,3 \
UNION \
SELECT 'Python' as Process_Name, 'Staging' as Table_Type, 'STG_Labor_Stats_CUSR0000SA0' as TableName, count(*) as Records_Processed, max(current_dttm) as Process_Dttm FROM STG_Labor_Stats_CUSR0000SA0 GROUP BY 1,2,3 \
UNION \
SELECT 'Python' as Process_Name, 'Staging' as Table_Type, 'STG_COVID19_Datahub_LVL2' as TableName, count(*) as Records_Processed, max(current_dttm) as Process_Dttm FROM STG_COVID19_Datahub_LVL2 GROUP BY 1,2,3 \
UNION \
SELECT 'Python' as Process_Name, 'Staging' as Table_Type, 'STG_COVID19_Datahub_LVL3' as TableName, count(*) as Records_Processed, max(current_dttm) as Process_Dttm FROM STG_COVID19_Datahub_LVL3 GROUP BY 1,2,3 \
UNION \
SELECT 'Python' as Process_Name, 'Staging' as Table_Type, 'STG_Fuel_Production' as TableName, count(*) as Records_Processed, max(current_dttm) as Process_Dttm FROM STG_Fuel_Production GROUP BY 1,2,3 \
UNION \
SELECT 'Python' as Process_Name, 'Staging' as Table_Type, 'STG_TSA_TRAVEL' as TableName, count(*) as Records_Processed, max(current_dttm) as Process_Dttm FROM STG_TSA_TRAVEL GROUP BY 1,2,3 \
UNION \
SELECT 'Python' as Process_Name, 'Staging' as Table_Type, 'STG_Estimated_Inpatient_All' as TableName, count(*) as Records_Processed, max(current_dttm) as Process_Dttm FROM STG_TSA_TRAVEL GROUP BY 1,2,3 \
UNION \
SELECT 'Python' as Process_Name, 'Staging' as Table_Type, 'STG_Estimated_Inpatient_Covid' as TableName, count(*) as Records_Processed, max(current_dttm) as Process_Dttm FROM STG_Estimated_Inpatient_Covid GROUP BY 1,2,3 \
UNION \
SELECT 'Python' as Process_Name, 'Staging' as Table_Type, 'STG_Estimated_Icu' as TableName, count(*) as Records_Processed, max(current_dttm) as Process_Dttm FROM STG_Estimated_Icu GROUP BY 1,2,3 \
UNION \
SELECT 'Python' as Process_Name, 'Staging' as Table_Type, 'STG_Consumer_Sentiment_Index' as TableName, count(*) as Records_Processed, max(current_dttm) as Process_Dttm FROM STG_Consumer_Sentiment_Index GROUP BY 1,2,3;"

#Fetch the data from Teradata using Pandas Dataframe
pda = pd.read_sql(query,con)
copy_to_sql(df = pda, table_name = "ETL_Indicator_Proj_Audit", schema_name=params.SchemaName, if_exists = 'append')
print(pda)
