persistent-odbc
===============
Using [Persistent package](http://hackage.haskell.org/package/persistent) with ODBC-databases 
throw [HDBC-odbc](http://hackage.haskell.org/package/hdbc-odbc).

Thanks to Dmitry Olshansky at https://github.com/odr/persistent-odbc/
for doing the initial hookup to hdbc-odbc.

Supports Postgres, MySql, MS Sql Server and Oracle.

Limit and Offset are not yet supported in MS Sql Server and Oracle.

This requires the modified version of Persistent and Esqueleto in this repository to work.

How to get started using cabal >= 1.18
======================================
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

