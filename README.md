persistent-odbc
===============
Using [Persistent package](http://hackage.haskell.org/package/persistent) with ODBC-databases 
throw [HDBC-odbc](http://hackage.haskell.org/package/hdbc-odbc).

Thanks to Dmitry Olshansky at https://github.com/odr/persistent-odbc/
for doing the initial hookup to hdbc-odbc.

Supports Postgres, MySql, MS Sql Server, Oracle and DB2.

see TestODBC.hs for usage and tests.

##How to get started using cabal >= 1.18
--------------------------------------
    git clone https://github.com/gbwey/persistent-odbc
    git clone https://github.com/gbwey/persistent
    git clone https://github.com/gbwey/esqueleto
    cd persistent-odbc
    cabal sandbox delete
    cabal sandbox init
    cabal sandbox add-source ..\persistent\persistent
    cabal sandbox add-source ..\esqueleto
    cabal install --only-dependencies
    -- assuming all goes well then ...
    cabal repl
    
    :l TestODBC.hs  [[after creating/changing the odbc system dsns]]
    :main s  [[test with mssql 2012]]
    :main so [[test with mssql pre 2012]]
    :main o  [[test with oracle pre 12c]]
    :main on [[test with oracle >=12c]]
    :main p  [[test with postgres]]
    :main m  [[test with mysql]]
    :main d  [[test with db2]]

####Limit and Offset in Ms Sql Server and Oracle
--------------------------------------------
use MSSQL True if you have MS Sql Server 2012 which has limit and offset support (esqueleto as well)
MSSQL False will support Limit only using persistent select

use Oracle True if you have Oracle >=12c which has limit and offset support (esqueleto as well)
use Oracle False if you have Oracle <12c where there is no limit and offset support 

####Ms Sql Server and Blobs
-----------------------
blobs don't support nulls (both insert and select) in this version of persistent-odbc (also doesnt work in hdbc-odbc)
may have problems with blobs and latest 2012 odbc driver (am using an older version)

####Ms Sql Server and deleteCascadeWhere
------------------------------------
can cause segfault in Ms Sql Server

####Oracle and nulls
----------------
treats empty string as a null (oracle thing)

####Oracle and sorting blobs
------------------------
cannot sort on a blob field (oracle thing)

####DB2 and Blobs
-----------------------
blobs don't support nulls (both insert) in this version of persistent-odbc (also doesnt work in hdbc-odbc)
select returns the blob values as hexstrings at the moment
