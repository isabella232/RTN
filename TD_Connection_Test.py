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
SchemaName = '???'

#############################################################
# Libraries
#############################################################
#pip install teradataml
from teradataml.dataframe.dataframe import DataFrame, in_schema
from teradataml.dataframe.copy_to import copy_to_sql
from teradataml.context.context import create_context, remove_context, get_connection, get_context
from teradataml.options.display import display
from teradataml.dataframe.fastload import fastload
from teradataml.context.context import *
import pandas as pd
con = create_context(host=MyHost, username=MyUser, password=getpass.getpass(), temp_database_name=SchemaName, logmech='LDAP')

#############################################################
# Teradata Query Test
#############################################################

query = "Select * from DBC.DBCInfo"

#Fetch the data from Teradata using Pandas Dataframe
pda = pd.read_sql(query,con)
print(pda)

