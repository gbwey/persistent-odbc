{-# LANGUAGE QuasiQuotes, TemplateHaskell, TypeFamilies, OverloadedStrings #-}
{-# LANGUAGE GADTs, FlexibleContexts #-}
{-# LANGUAGE EmptyDataDecls    #-}
{-# LANGUAGE FlexibleInstances, MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables, GeneralizedNewtypeDeriving, DeriveGeneric #-}
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
    let dbtype::DBType
        dbtype=read $ T.unpack $ connRDBMS conn
    liftIO $ putStrLn $ "original:" ++ show dbtype' ++ " calculated:" ++ show dbtype
    liftIO $ putStrLn "\nbefore migration\n"
    runMigration migrateAll
    liftIO $ putStrLn "after migration"
    
