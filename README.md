# RefreshNonProdSqlDatabases

This technique details the process to take a production SQL database and automatically transform it into a development database, 
with steps including:
- dropping the existing DEV database in Sandbox server
- copy over the latest PROD database from PROD backup and restore to Sandbox server
- change the name of the database to DEV suffix, if applicable
- find biggest tables, and only keep 1 - 2 months worth of data
- mask financially sensitive and PII data
- shrink and reindex DEV database
- backup database to shared directory
- drop database, if applicable

This is part of the PASS Data Community Summit in Seattle Washington in November, 2022.
Recording and Powerpoint presentation included.

Patent Application Pending
