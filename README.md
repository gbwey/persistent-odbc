persistent-odbc
===============
Using [Persistent package](http://hackage.haskell.org/package/persistent) with ODBC-databases 
throw [HDBC-odbc](http://hackage.haskell.org/package/hdbc-odbc).

Thanks to Dmitry Olshansky at https://github.com/odr/persistent-odbc/
for doing the initial hookup to hdbc-odbc.

Supports Postgres, MySql, MS Sql Server, Oracle and DB2.

see TestODBC.hs for usage and tests.

##How to get started using cabal >= 1.18
    git clone https://github.com/gbwey/persistent-odbc
    git clone https://github.com/gbwey/persistent
    git clone https://github.com/gbwey/esqueleto -b test8
    cd persistent-odbc
    cabal sandbox delete
    cabal sandbox init
    cabal sandbox add-source ../persistent/persistent
    cabal sandbox add-source ../esqueleto
    cabal install --only-dependencies --flags="tester"
    cabal configure --flags="tester"
    cabal repl
    
    :load TestODBC.hs  (also need to define the appropriate odbc system dsn below and then run the command)
    
    e.g. for postgresql
        1. define the system odbc dsn pg_test
        2. :main p  

| Command | DBMS | ODBC System Dsn | Notes |
| ------------- |:------------- |:----- |:----- |
| :main p      | Postgresql | dsn=pg_test | |
| :main m      | MySQL | dsn=mysql_test | |
| :main d      | DB2 | dsn=db2_test | |
| :main s      | Ms Sql Server | dsn=mssql_test; Trusted_Authentication=True; | >= 2012 |
| :main so      | Ms Sql Server | dsn=mssql_test; Trusted_Authentication=True; | pre 2012 |
| :main o      | Oracle | dsn=oracle_test | pre 10c |
| :main on      | Oracle | dsn=oracle_test | >= 10c |
 
####Limit and Offset in Ms Sql Server and Oracle
  MSSQL True for MS Sql Server 2012 which has limit and offset support (esqueleto as well)
  MSSQL False for MS Sql Server pre 2012 will only supports Limit using persistent's select (select top n ...)

  Oracle True for Oracle >=12c which has limit and offset support (esqueleto as well)
  Oracle False for Oracle <12c where there is no limit and offset support 

####Ms Sql Server and Blobs
  Blobs don't support nulls (both insert and select) in this version of persistent-odbc (also doesn't work in hdbc-odbc)
  You may have problems with blobs and latest 2012 odbc driver

####Ms Sql Server and deleteCascadeWhere
  Can cause segfault in Ms Sql Server

####Oracle and nulls
  Treats empty string as a null (oracle thing)

####Oracle and sorting blobs
  Cannot sort on a blob field (oracle thing)

####DB2 and Blobs
  Blobs don't support nulls (both insert and select) in this version of persistent-odbc (also doesn't work in hdbc-odbc)
  Select returns the blob values as unpacked strings at the moment (if there is interest in getting this fixed let me know)
