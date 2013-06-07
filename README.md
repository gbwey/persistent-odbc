persistent-odbc
===============
Using [Persistent package](http://hackage.haskell.org/package/persistent) with ODBC-databases 
throw [HDBC-odbc](http://hackage.haskell.org/package/hdbc-odbc).

Functionality is very bounded because there are no schema modification possibilities in general. 

Beside that, `Persistent` has some assumptions about table structures which is not always possible to provide in all databases.
E.g. `hdbc-odbc` transform Oracle's `NUMBER` into `SqlString` value, 
but `Persistent` want to have `PersistInt64` value for keys. I don't see a way to provide that.

Anyway, with this package we can do at least low-level queries from `Persistent` to any databases.
