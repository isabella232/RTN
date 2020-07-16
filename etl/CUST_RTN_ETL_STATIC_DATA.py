#!/usr/bin/env python
# coding: utf-8

# In[ ]


#!/usr/bin/env python
# coding: utf-8


#############################################################
# Credentials
#############################################################
MyHost ='???'
MyUser ='???'
TempDB ='???'
SchemaName = "???"

#############################################################
# Libraries
#############################################################
#pip install ipython
#pip install --upgrade covid19dh
#pip install numpy --upgrade
#pip install requests
#pip3 install tabulate
#pip install xlrd
#pip3 install requests-html
#pip install html5lib

from IPython import get_ipython
from covid19dh import covid19
import pytz
import tweepy as tw
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
con = create_context(host=MyHost, username=MyUser, password=getpass.getpass(), temp_database_name=TempDB, logmech='LDAP')

#############################################################
# Manual Loads
#############################################################

# DIM_GEO_LOCATION_T
url = 'https://raw.consulting-github.teradata.com/Vantage-Demos/Covid19/master/data/DIM_GEO_LOCATION_T.txt?token=AAABQL8l3HTLCLBDZcoSPJKU1g1elHlGks5fE1QWwA%3D%3D'
df = pd.read_csv(url,sep="|", doublequote=True,  encoding='latin-1')
copy_to_sql(df = df, table_name = "TEMP_DIM_GEO_LOCATION_T", schema_name = SchemaName, if_exists = 'replace')
pd.read_sql('DATABASE '+SchemaName,con)
pd.read_sql('DELETE FROM DIM_GEO_LOCATION_T;',con)
pd.read_sql('INSERT INTO DIM_GEO_LOCATION_T SELECT * FROM TEMP_DIM_GEO_LOCATION_T;',con)
            
# DIM_ZIP_COUNTY_MSA_MAP_RAW
url = 'https://raw.consulting-github.teradata.com/Vantage-Demos/Covid19/master/data/DIM_ZIP_COUNTY_MSA_MAP_RAW.txt?token=AAABQO50AU3OFVpe3_Xrhove0BT1gCrCks5fE2KTwA%3D%3D'
df = pd.read_csv(url,sep="|", doublequote=True,  encoding='latin-1')
copy_to_sql(df = df, table_name = "TEMP_DIM_ZIP_COUNTY_MSA_MAP_RAW", schema_name = SchemaName, if_exists = 'replace')
pd.read_sql('DELETE FROM DIM_ZIP_COUNTY_MSA_MAP_RAW;',con)
pd.read_sql('INSERT INTO DIM_ZIP_COUNTY_MSA_MAP_RAW SELECT CAST(ZIPCODE as VARCHAR(10)), CAST(COUNTY_FIPS as VARCHAR(10)), COUNTY_RES_RATIO, COUNTY_BUS_RATIO, COUNTY_OTH_RATIO, COUNTY_TOT_RATIO, TOP_ZIP_FLAG, SPLIT_COUNTY_CNT, TOP_COUNTY_FLAG, COUNTY_NAME, CBSA_CODE, CBSA_NAME, MSA_NAME, STATE_NAME FROM TEMP_DIM_ZIP_COUNTY_MSA_MAP_RAW;',con)

# DIM_People_location
url = 'https://raw.consulting-github.teradata.com/Vantage-Demos/Covid19/master/data/DIM_People_location.txt?token=AAABQIUJaIx0F6cLeciltI9Am5vn-5HZks5fE2TkwA%3D%3D'
df = pd.read_csv(url,sep="|", doublequote=True,  encoding='latin-1')
copy_to_sql(df = df, table_name = "TEMP_DIM_People_location", schema_name = SchemaName, if_exists = 'replace')
pd.read_sql('DELETE FROM DIM_People_location;',con)
pd.read_sql('INSERT INTO DIM_People_location SELECT CLient_nbr, Client_name, Site_Id, Site_Type, Address, City, State, CAST(Zipcode as VARCHAR(10)), Region, Country_cd, Nbr_at_location, People_type FROM TEMP_DIM_People_location;',con)

# DIM_Site_addresses
url = 'https://raw.consulting-github.teradata.com/Vantage-Demos/Covid19/master/data/DIM_Site_addresses.txt?token=AAABQGI5HQyuzQrhBLKxFhJyNlc01Nguks5fE2UFwA%3D%3D'
df = pd.read_csv(url,sep="|", doublequote=True,  encoding='latin-1')
copy_to_sql(df = df, table_name = "TEMP_DIM_Site_addresses", schema_name = SchemaName, if_exists = 'replace')
pd.read_sql('DELETE FROM DIM_Site_addresses;',con)
pd.read_sql('INSERT INTO DIM_Site_addresses SELECT * FROM TEMP_DIM_Site_addresses;',con)

# STG_CO_EST2019
url = 'https://raw.consulting-github.teradata.com/Vantage-Demos/Covid19/master/data/STG_CO_EST2019.txt?token=AAABQMUhYIWDgIFHKZrVSxOl9SdfefV6ks5fE2atwA%3D%3D'
df = pd.read_csv(url,sep="|", doublequote=True,  encoding='latin-1')
copy_to_sql(df = df, table_name = "TEMP_STG_CO_EST2019", schema_name = SchemaName, if_exists = 'replace')
pd.read_sql('DELETE FROM STG_CO_EST2019;',con)
pd.read_sql('INSERT INTO STG_CO_EST2019 SELECT * FROM TEMP_STG_CO_EST2019;',con)

# Transaltion_Table
url = 'https://raw.consulting-github.teradata.com/Vantage-Demos/Covid19/master/data/Transaltion_Table.txt?token=AAABQNwyni4UPn0XzznytaicNmgubVU3ks5fE8_twA%3D%3D'
df = pd.read_csv(url,sep="|", doublequote=True,  encoding='latin-1')
copy_to_sql(df = df, table_name = "TEMP_Transaltion_Table", schema_name = SchemaName, if_exists = 'replace')
pd.read_sql('DELETE FROM Transaltion_Table;',con)
pd.read_sql('INSERT INTO Transaltion_Table SELECT * FROM TEMP_Transaltion_Table;',con)

# STG_BEA_PersonalConsumption_2_3_5
url = 'https://raw.consulting-github.teradata.com/Vantage-Demos/Covid19/master/data/STG_BEA_PersonalConsumption_2_3_5.txt?token=AAABQDsdhYvVirmWLFCM_fI3LPHMZCJ2ks5fFMAjwA%3D%3D'
df = pd.read_csv(url,sep="|", doublequote=True,  encoding='latin-1')
copy_to_sql(df = df, table_name = "TEMP_STG_BEA_PersonalConsumption_2_3_5", schema_name = SchemaName, if_exists = 'replace')
pd.read_sql('DELETE FROM STG_BEA_PersonalConsumption_2_3_5;',con)
pd.read_sql('INSERT INTO STG_BEA_PersonalConsumption_2_3_5 SELECT * FROM TEMP_STG_BEA_PersonalConsumption_2_3_5;',con)

# STG_Consumer_Sentiment_Index
url = 'https://raw.consulting-github.teradata.com/Vantage-Demos/Covid19/master/data/STG_Cunsumer_Sentiment_Index.txt?token=AAABQH5_aFHYzS-S5LXiEHp5ksbj1PbMks5fGHtbwA%3D%3D'
df = pd.read_csv(url,sep="|", doublequote=True,  encoding='latin-1')
copy_to_sql(df = df, table_name = "TEMP_STG_Consumer_Sentiment_Index", schema_name = SchemaName, if_exists = 'replace')
pd.read_sql('DELETE FROM STG_Consumer_Sentiment_Index ;',con)
pd.read_sql('INSERT INTO STG_Consumer_Sentiment_Index SELECT * FROM TEMP_STG_Consumer_Sentiment_Index;',con)


from datetime import datetime
datetime.utcnow()
dateTimeObj = pytz.utc.localize(datetime.utcnow()).astimezone(pytz.timezone('US/Pacific'))
timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
print("Initial Static Tables Reloaded!  " + timestampStr)