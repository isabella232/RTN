Copyright (C) 2016-2020 by Teradata Corporation. All rights reserved.
# Covid19

Covid19 Return to Business demo assets

This application consists of the following components:
  * Database Objects Installation - RTN_Install.ddl (ddl folder)
  * Database Objects Uninstall - RTN_Uninstall.ddl (ddl folder)
  * ETL Python Script - RTN_ETL.py (etl folder)
  * Tableau Application Visualization (tableau folder)
  * Initial Manual Data Inserts (data folder)
  
# Prerequisites:
  * Python Version 3.7 or Greater
  * Teradata DBS 16.20 FU or Greater
  * Tableau Version 2020.1
  * Tableau Reader Version 2020.1  (download at tableau.com)
  
# Configuation Steps:
  * Set Database Credential Information in the file RTN_ETL.py
      * MyHost ='???'
      * MyUser ='???'
      * TempDB ='???'
      * SchemaName = '???'
  * Email Notification SMPT Server Info
      * sender_email = '???'
      * receivers = ['???','???',...]
      * password = '???'
  * Set Default Database in the DDL Installation File ddl/RTN.ddl
      * Do a complete replacement of '?' to target database name'
      
# Installation Steps:
  * Initiate a Teradata session through SQL Assistant, Teradata Studio or BTEQ
  * Run RTN_Install.ddl Install through successful completion
  * Check the logs and database for additional Tables/Views/Stored Procedures
  * Run Python Script RTN_ETL.py for initial static table load as well first initial load
  * Schedule the Python Script Load to be run Daily or manually run for updates
  
# Tableau Dashboard Refresh:
  * Tableau requires refresh process after initial load for each of the 11 data sources
  * Right mouse on the data source and select Extract -> Refresh
  * You will be prompted to log into the database in order to refresh the data source 

# Audit Process/Validation:
  * Post every run, an email is generated with all the daily load statitics and tables/processes requiring attention
  * Audit Table ETL_Indicator_Proj_Audit captures all the audit records counts and timestamp from Source systems into Teradata staging tables
  * Audit Table ETL_Indicator_Proj_Audit captures all the audit records counts and timestamp from Teradata Staging to Core
  * Error Table ETL_Proc_Error_Logs, captures all stage to core stored procedure errors
  
# To Uninstall:
  * Set Default Database in the DDL Installation File ddl/RTN_Uninstall.ddl
      * Do a complete replacement of '?' to target database name
  * Run RTN_Uninstall.ddl which removes all Tables/Views/Stored Procedure from your database
