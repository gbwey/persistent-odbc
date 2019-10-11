{-# OPTIONS -Wall #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE EmptyDataDecls #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DeriveGeneric #-}
module TestOne where

import qualified Database.Persist as P
import Database.Persist.TH
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Logger
import Control.Monad.Trans.Resource (runResourceT,MonadResource)
import Control.Monad.Trans.Reader (ask)
import Data.Text (Text)
import qualified Data.Text as T
import Database.Persist.ODBC
--import Data.Conduit
import Data.Aeson
import System.Environment (getArgs)

import qualified Database.Esqueleto as E
import Database.Esqueleto (select,where_,(^.),from,Value(..))
import Control.Applicative ((<$>),(<*>))
import Data.Int
import Debug.Trace
import Control.Monad (when)

share [mkPersist sqlSettings, mkMigrate "migrateAll"] [persistLowerCase|
Parent99
    name String
    deriving Show
Child99
    refParent99 Parent99Id
    deriving Show

Parent7
    name  String maxlen=20
    name2 String maxlen=20
    age Int
    Primary name name2 age
    deriving Show Eq
Child7
    name  String maxlen=20
    name2 String maxlen=20
    age Int
    extra Text maxlen=50
    Foreign Parent7 fkparent7 name name2 age
    deriving Show Eq
|]

main :: IO ()
main = do
  [arg] <- getArgs
  let (dbtype',dsn) =
       case arg of -- odbc system dsn
           "d" -> (DB2,"dsn=db2_test")
           "p" -> (Postgres,"dsn=pg_test")
           "m" -> (MySQL,"dsn=mysql_test")
           "s" -> (MSSQL True,"dsn=mssql_test; Trusted_Connection=True") -- mssql 2012 [full limit and offset support]
           "so" -> (MSSQL False,"dsn=mssql_test; Trusted_Connection=True") -- mssql pre 2012 [limit support only]
           "o" -> (Oracle False,"dsn=oracle_test") -- pre oracle 12c [no support for limit and offset]
           "on" -> (Oracle True,"dsn=oracle_test") -- >= oracle 12c [full limit and offset support]
           "q" -> (Sqlite False,"dsn=sqlite_test")
           "qn" -> (Sqlite True,"dsn=sqlite_test")
           xs -> error $ "unknown option:choose p m s so o on d q qn found[" ++ xs ++ "]"

  runResourceT $ runNoLoggingT $ withODBCConn Nothing dsn $ runSqlConn $ do
    conn <- ask
    let dbtype :: DBType
        dbtype=read $ T.unpack $ connRDBMS conn
    liftIO $ putStrLn $ "original:" ++ show dbtype' ++ " calculated:" ++ show dbtype
    liftIO $ putStrLn "\nbefore migration\n"
    runMigration migrateAll
    liftIO $ putStrLn "after migration"
{-
    p1 <- insert $ Parent7 "k1a" "k1b" 100
    p2 <- insert $ Parent7 "k2a" "k2b" 200
    p3 <- insert $ Parent7 "k3a" "k3b" 300

    c1 <- insert $ Child7 "k1a" "k1b" 100 "extra11"
    c1a <- insert $ Child7 "k1a" "k1b" 100 "extra11aa"
    c2 <- insert $ Child7 "k2a" "k2b" 200 "extra11"
    --c2x <- insert $ Child7 "k2a" "k2b" 900 "extra33"
-}
{-
    liftIO $ putStrLn "before esqueleto test 1"
    xs <- select $
           from $ \p -> do
              where_ (p ^. Parent7Name2 E.==. E.val "k1b")
              return p
    liftIO $ putStrLn $ "xs=" ++ show xs
-}
    liftIO $ putStrLn "before esqueleto test 2"
    ys <- select $
           from $ \(p,c) -> do
              where_ (p ^. Parent7Name2 E.==. c ^. Child7Name2)
              return (p,c)
    liftIO $ putStrLn $ "ys=" ++ show ys


    liftIO $ putStrLn "before esqueleto test 3"
    bs <- select $
                 from $ \(c `E.InnerJoin` p) -> do
                 E.on $ p ^. Parent7Name E.==. c ^. Child7Name
                 return (p,c)
    liftIO $ putStrLn $ "bs=" ++ show bs

{-
    liftIO $ putStrLn "before esqueleto test 4"
    zs <- select $
           from $ \p -> do
              where_ (p ^. Parent7Id E.==. E.valkey (Parent7 "k1a" "k1b" 100))
              return p
    liftIO $ putStrLn $ "zs=" ++ show zs

    liftIO $ putStrLn "before esqueleto test 5"
    ws <- select $
           from $ \(p,c) -> do
              where_ (c ^. Child7Extra E.==. E.val "extra11" E.&&. p ^. Parent7Id E.==. c ^. Child7Parent7)
              return (p,c)
    liftIO $ putStrLn $ "ws=" ++ show ws
-}
    return ()

{-
before esqueleto test 2
*** Exception: SqlError {seState = "[\"42S22\"]", seNativeError = -1, seErrorMsg
 = "execute execute: [\"1054: [MySQL][ODBC 5.2(a) Driver][mysqld-5.6.13-log]Unkn
own column 'parent7.id' in 'field list'\"]"}
-}