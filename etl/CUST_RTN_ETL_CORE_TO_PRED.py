#!/usr/bin/env python
# coding: utf-8


# importing  library

import pandas as pd
import teradataml as tml
from teradataml import create_context, remove_context,DataFrame,copy_to_sql,get_connection,ScaleMap,Scale 
import getpass
import os
import numpy as np


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

from teradataml.analytics.mle.Arima import Arima
from teradataml.analytics.mle.ArimaPredict import ArimaPredict
from teradataml.analytics.mle.VarMax import VarMax
import params

# Connection 

vantage = create_context(host=params.MyHost, username=params.MyUser, password=params.Password, temp_database_name=params.SchemaName, logmech=params.LogMech)


#############################################################
#Fetching Training Data for Cases in State
#############################################################

vac1_u = "SELECT * from " +params.SchemaName +".US_CONF_STATE_VAC_SMAVG"

#############################################################
#Converting to Dataframe
#############################################################

vac1_u_df = DataFrame.from_query(vac1_u)

#######################################################################
#Building Varmax Model for Prediction of Confirmed Cases in US States
#######################################################################

varmax_out3 = VarMax(data = vac1_u_df,
                        data_partition_column = ["PROVINCE_STATE"],
                        data_order_column = ["DATE"],
                        response_columns = ["TOTAL_CONF_smavg"],
                        exogenous_columns = ["TOTAL_VAC"],
                        partition_columns = ["PROVINCE_STATE"],
                        orders = "3,0,2",
                        seasonal_orders = "3,1,2",
                        period = 1,
                        exogenous_order = 3,
                        lag = 1,
                        include_mean = False,
                        step_ahead = 90
                        )


#############################################################
# Loading results to database
#############################################################

copy_to_sql(varmax_out3.result, table_name="varmax_case_vac", schema_name = params.SchemaName ,if_exists="replace")

print("Prediction of confirmed cases in US States!")


#############################################################
#Fetching Training Data for Deaths in State
#############################################################

vac2_u = "SELECT * from " +params.SchemaName +".US_DEATH_STATE_VAC_SMAVG"

#############################################################
#Converting to Dataframe
#############################################################

vac2_u_df = DataFrame.from_query(vac2_u)

####################################################################
#Building Varmax Model for Prediction of Death Cases in US States
####################################################################

varmax_out4 = VarMax(data = vac2_u_df,
                     data_partition_column = ["PROVINCE_STATE"],
                     data_order_column = ["DATE"],
                     response_columns = ["TOTAL_DEATHS_smavg"],
                     exogenous_columns = ["TOTAL_VAC"],
                     partition_columns = ["PROVINCE_STATE"],
                     orders = "3,0,2",
                     seasonal_orders = "3,1,2",
                     period = 1,
                     exogenous_order = 1,
                     lag = 1,
                     include_mean = False,
                     step_ahead = 90
                     )


#############################################################
# Loading results to database
#############################################################

copy_to_sql(varmax_out4.result, table_name="varmax_death_vac", schema_name = params.SchemaName ,if_exists="replace")

print("Prediction of death cases in US States!")


#############################################################
#Fetching Training Data for Cases in County
#############################################################

tct_u ="SELECT * from "  +params.SchemaName +".US_CONF_COUNTY_SMAVG"

#############################################################
#Fetching Training Data for Deaths in County
#############################################################

tdt_u ="SELECT * from " +params.SchemaName +".US_DEATH_COUNTY_SMAVG"

#############################################################
#Converting to Dataframe
#############################################################

tct_df_u = DataFrame.from_query(tct_u)

#############################################################
#Converting to Dataframe
#############################################################

tdt_df_u = DataFrame.from_query(tdt_u)

##########################################################################
#Building Arima Model for Prediction of Confirmed Cases in US Counties
##########################################################################

arima_out5 = Arima(data = tct_df_u,
                     timestamp_columns = ["date"],
                     value_column ="TOT_CONF_smavg",
                     partition_columns ="County",                         
                     order = "0,1,2",
                     seasonal_order_p = 3,
                     seasonal_order_d = 1,
                     seasonal_order_q = 2, 
                     period = 1)


arima_predict_out5 = ArimaPredict(object = arima_out5,
                                        object_partition_column='county',
                                        residual_table=arima_out5.residual_table,
                                        residual_table_partition_column='county',
                                        residual_table_order_column='date',
                                        partition_columns='county',
                                        n_ahead= 90)


#############################################################
# Loading results to database
#############################################################

copy_to_sql(arima_predict_out5.result, table_name="arima_county_case", schema_name = params.SchemaName ,if_exists="replace")

print("Prediction of confirmed cases in US Counties!")


#####################################################################
#Building Arima Model for Prediction of Death Cases in US Counties
#####################################################################

arima_out6 = Arima(data = tdt_df_u,
                     timestamp_columns = ["date"],
                     value_column ="TOT_DEATHS_smavg",
                     partition_columns ="County",                         
                     order = "0,1,2",
                     seasonal_order_p = 3,
                     seasonal_order_d = 1,
                     seasonal_order_q = 2, 
                     period = 1)


arima_predict_out6 = ArimaPredict(object = arima_out6,
                                        object_partition_column='county',
                                        residual_table=arima_out6.residual_table,
                                        residual_table_partition_column='county',
                                        residual_table_order_column='date',
                                        partition_columns='county',
                                        n_ahead= 90)


#############################################################
# Loading results to database
#############################################################

copy_to_sql(arima_predict_out6.result, table_name="arima_county_death", schema_name = params.SchemaName ,if_exists="replace")

print("Prediction of death cases in US Counties!")


###################
# ECONOMIC FACTORS
###################

#############################################################
#Fetching Economic Factors with Weekly data
#############################################################

ef_trn = "sel * from " +params.SchemaName + ".ECONOMIC_FACTORS_TRN"

#############################################################
#Fetching Economic Factors with Monthly data
#############################################################

ef_nh_trn = "sel * from " +params.SchemaName + ".ECONOMIC_FACTORS_NH_TRN"

#############################################################
#Converting to Dataframe
#############################################################

ef_nh_trn = DataFrame.from_query(ef_nh_trn)

ef_trn = DataFrame.from_query(ef_trn)

#############################################################
#Predicting Economic Factors with Weekly data
#############################################################

arima_out5 = Arima(data = ef_trn,
                     timestamp_columns = ["DATE_KEY"],
                     value_column ="METRIC_VALUE",
                     partition_columns ="SUBDOMAIN_1_NAME",
                     order = "0,1,2",
                     seasonal_order_p = 0,
                     seasonal_order_d = 1,
                     seasonal_order_q = 2, 
                     period = 12)


arima_predict_out5 = ArimaPredict(object = arima_out5,
                                        object_partition_column='subdomain_1_name',
                                        residual_table=arima_out5.residual_table,
                                        residual_table_partition_column='subdomain_1_name',
                                        residual_table_order_column='date_key',
                                        partition_columns='subdomain_1_name',
                                        n_ahead= 30)


#############################################################
#Predicting Economic Factors with Monthly Data 
#############################################################

arima_out7 = Arima(data = ef_nh_trn,
                     timestamp_columns = ["DATE_KEY"],
                     value_column ="METRIC_VALUE",
                     partition_columns ="SUBDOMAIN_1_NAME",
                     order = "1,1,2",
                     seasonal_order_p = 0,
                     seasonal_order_d = 1,
                     seasonal_order_q = 2, 
                     period = 3)


arima_predict_out7 = ArimaPredict(object = arima_out7,
                                        object_partition_column='subdomain_1_name',
                                        residual_table=arima_out7.residual_table,
                                        residual_table_partition_column='subdomain_1_name',
                                        residual_table_order_column='date_key',
                                        partition_columns='subdomain_1_name',
                                        n_ahead= 12)


#############################################################
# Loading results to database
#############################################################

copy_to_sql(arima_predict_out5.result, table_name="EF_ARIMA_PRED", schema_name = params.SchemaName ,if_exists="replace")
print("Prediction of economic factors with weekly data!")

copy_to_sql(arima_predict_out7.result, table_name="EF_ARIMA_PRED_NH", schema_name = params.SchemaName ,if_exists="replace")
print("Prediction of economic factors with monthly data!")
