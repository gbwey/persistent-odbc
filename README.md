persistent-odbc
===============
Uses the [Persistent package](http://hackage.haskell.org/package/persistent) and [HDBC-odbc](http://hackage.haskell.org/package/HDBC-odbc)
to access databases via ODBC.

Thanks to Dmitry Olshansky at <https://github.com/odr/persistent-odbc/>
for doing the initial hookup to hdbc-odbc.

Supports Postgres, MySql, MS Sql Server, Oracle, DB2, and SQLite.

see TestODBC.hs for usage and tests.

## How to get started using cabal >= 1.18

```text
git clone https://github.com/gbwey/persistent-odbc
cd persistent-odbc
stack ghci --test --flag persistent-odbc:tester
:load TestODBC.hs  (also need to define the appropriate odbc system dsn below and then run the command)

e.g. for postgresql
1. create a system odbc dsn "pg_test"
2. :main p
```

| DBMS                   | ODBC System Dsn | Command |
| ---------------------- |:--------------- |:------- |
| Postgresql             | dsn=pg_test | :main p |
| MySQL                  | dsn=mysql_test | :main m |
| DB2                    | dsn=db2_test | :main d |
| Ms Sql Server >= 2012  | dsn=mssql_test | :main s |
| Ms Sql Server pre 2012 | dsn=mssql_test | :main so |
| Oracle pre 12c         | dsn=oracle_test | :main o |
| Oracle >= 12c          | dsn=oracle_test | :main on |
| Sqlite                 | dsn=sqlite_test | :main q |

#### Limit and Offset in Ms Sql Server and Oracle

  MSSQL True for MS Sql Server 2012 which has limit and offset support (esqueleto as well).
  MSSQL False for MS Sql Server pre 2012 only supports limit using Persistent's select* operations. (select top n ...).

  Oracle True for Oracle >=12c which has limit and offset support (esqueleto as well).
  Oracle False for Oracle <12c where there is no limit and offset support.

#### Ms Sql Server and blobs

  Blobs are problematic in hdbc-odbc.

#### Ms Sql Server and deleteCascadeWhere

  Can cause segfault in Ms Sql Server.

#### Oracle and nulls

  Treats empty string as a null (oracle issue).

#### Oracle and sorting blobs

  Cannot sort on a blob field (oracle issue).

#### DB2 and blobs

  Blobs don't support nulls (both insert and select) in this version of persistent-odbc (also doesn't work in hdbc-odbc).
  Select returns the blob values as unpacked strings at the moment (if there is interest in getting this fixed let me know).
