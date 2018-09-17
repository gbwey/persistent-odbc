{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses, ScopedTypeVariables #-}
-- | An ODBC backend for persistent.
module Database.Persist.ODBC
    ( withODBCPool
    , withODBCConn
    , createODBCPool
    , module Database.Persist.Sql
    , ConnectionString
    , OdbcConf (..)
    , openSimpleConn
    , DBType (..)
    , mysql,postgres,mssqlMin2012,mssql,oracleMin12c,oracle,db2,sqlite
    ) where

import qualified Control.Exception as E -- (catch, SomeException)
import Database.Persist.Sql
import qualified Database.Persist.MigratePostgres as PG
import qualified Database.Persist.MigrateMySQL as MYSQL
import qualified Database.Persist.MigrateMSSQL as MSSQL
import qualified Database.Persist.MigrateOracle as ORACLE
import qualified Database.Persist.MigrateDB2 as DB2
import qualified Database.Persist.MigrateSqlite as SQLITE

import Data.Time(ZonedTime(..))

import qualified Database.HDBC.ODBC as O
import qualified Database.HDBC as O
import qualified Database.HDBC.SqlValue as HSV
import qualified Data.Convertible as DC

import Control.Monad.IO.Class (MonadIO (..))
import Control.Monad.Trans.Resource (MonadUnliftIO)
import Data.IORef(newIORef)
import qualified Data.Map as Map
import qualified Data.Text as T
import Data.Time.LocalTime (localTimeToUTC, utc)
import Data.Text (Text)
import Data.Aeson -- (Object(..), (.:))
import Control.Monad (mzero)
--import Control.Monad.Trans.Resource (MonadResource)
import Control.Monad.Logger

import Data.Int (Int64)
import Data.Conduit
import Database.Persist.ODBCTypes
import qualified Data.List as L
import Data.Acquire (Acquire, mkAcquire)
-- | An @HDBC-odbc@ connection string.  A simple example of connection
-- string would be @DSN=hdbctest1@. 
type ConnectionString = String

-- | Create an ODBC connection pool and run the given
-- action.  The pool is properly released after the action
-- finishes using it.  Note that you should not use the given
-- 'ConnectionPool' outside the action since it may be already
-- been released.
withODBCPool :: (MonadUnliftIO m, MonadLogger m)
             => Maybe DBType 
             -> ConnectionString
             -- ^ Connection string to the database.
             -> Int
             -- ^ Number of connections to be kept open in
             -- the pool.
             -> (ConnectionPool -> m a)
             -- ^ Action to be executed that uses the
             -- connection pool.
             -> m a
withODBCPool dbt ci = withSqlPool (\lg -> open' lg dbt ci)


-- | Create an ODBC connection pool.  Note that it's your
-- responsibility to properly close the connection pool when
-- unneeded.  Use 'withODBCPool' for an automatic resource
-- control.
createODBCPool :: (MonadUnliftIO m, MonadLogger m)
               => Maybe DBType 
               -> ConnectionString
               -- ^ Connection string to the database.
               -> Int
               -- ^ Number of connections to be kept open
               -- in the pool.
               -> m ConnectionPool
createODBCPool dbt ci = createSqlPool (\lg -> open' lg dbt ci)

-- | Same as 'withODBCPool', but instead of opening a pool
-- of connections, only one connection is opened.
withODBCConn :: (MonadUnliftIO m, MonadLogger m)
             => Maybe DBType -> ConnectionString -> (SqlBackend -> m a) -> m a
withODBCConn dbt cs = withSqlConn (\lg -> open' lg dbt cs)

-- | helper function that returns a connection based on the database type
open' :: LogFunc -> Maybe DBType -> ConnectionString -> IO SqlBackend
open' logFunc mdbtype cstr = 
    O.connectODBC cstr >>= openSimpleConn logFunc mdbtype

-- | returns a supported database type based on its version 
-- if the user does not provide the database type explicitly I look it up based on connection metadata
findDBMS::(String, String, String) -> DBType
findDBMS dvs@(driver,ver,serverver) 
    | driver=="Oracle" = Oracle $ getServerVersionNumber dvs>=12
    | "DB2" `L.isPrefixOf` driver = DB2 
    | driver=="Microsoft SQL Server" = MSSQL $ getServerVersionNumber dvs>=11
    | driver=="MySQL" = MySQL 
    | "PostgreSQL" `L.isPrefixOf` driver = Postgres  
    | "SQLite" `L.isPrefixOf` driver = Sqlite False
    | otherwise = error $ "unknown or unsupported driver[" ++ driver ++ "] ver[" ++ ver ++ "] serverver[" ++ serverver ++ "]\nExplicitly set the type of dbms using DBType and try again!"

-- | extracts the server version number 
getServerVersionNumber::(String, String, String) -> Integer
getServerVersionNumber (driver, ver, serverver) = 
      case reads $ takeWhile (/='.') serverver of
        [(a,"")] -> a
        xs -> error $ "getServerVersionNumber of findDBMS:cannot tell the version xs=" ++show xs ++ ":" ++ "driver[" ++ driver ++ "] ver[" ++ ver ++ "] serverver[" ++ serverver ++ "]"


-- | Generate a persistent 'Connection' from an odbc 'O.Connection'
openSimpleConn :: LogFunc -> Maybe DBType -> O.Connection -> IO SqlBackend
openSimpleConn logFunc mdbtype conn = do
    let mig=case mdbtype of 
              Nothing -> getMigrationStrategy $ findDBMS (O.proxiedClientName conn, O.proxiedClientVer conn, O.dbServerVer conn) 
              Just dbtype -> getMigrationStrategy dbtype
      
    smap <- newIORef Map.empty
    return SqlBackend
        { connLogFunc       = logFunc
        , connPrepare       = prepare' conn
        , connStmtMap       = smap
        , connInsertSql     = dbmsInsertSql mig
        , connInsertManySql = Nothing
        , connClose         = O.disconnect conn
        , connMigrateSql    = dbmsMigrate mig
        , connBegin         = const 
                     $ E.catch (O.commit conn) (\(_ :: E.SomeException) -> return ())  
            -- there is no nested transactions.
            -- Transaction begining means that previous commited
        , connCommit        = const $ O.commit   conn
        , connRollback      = const $ O.rollback conn
        , connEscapeName    = dbmsEscape mig
        , connNoLimit       = "" -- esqueleto uses this but needs to use connLimitOffset then we can dump this field
        , connRDBMS         = T.pack $ show (dbmsType mig)
        , connLimitOffset   = dbmsLimitOffset mig
        , connMaxParams     = Nothing
        , connUpsertSql     = Nothing
        , connPutManySql     = Nothing
        }
-- | Choose the migration strategy based on the user provided database type
getMigrationStrategy::DBType -> MigrationStrategy
getMigrationStrategy dbtype = 
  case dbtype of
    Postgres  -> PG.getMigrationStrategy dbtype
    MySQL     -> MYSQL.getMigrationStrategy dbtype
    MSSQL {}  -> MSSQL.getMigrationStrategy dbtype
    Oracle {} -> ORACLE.getMigrationStrategy dbtype
    DB2 {}    -> DB2.getMigrationStrategy dbtype
    Sqlite {} -> SQLITE.getMigrationStrategy dbtype

prepare' :: O.Connection -> Text -> IO Statement
prepare' conn sql = do
#if DEBUG
    putStrLn $ "Database.Persist.ODBC.prepare': sql = " ++ T.unpack sql
#endif
    stmt <- O.prepare conn $ T.unpack sql
    
    return Statement
        { stmtFinalize  = O.finish stmt
        , stmtReset     = return () -- rollback conn ?
        , stmtExecute   = execute' stmt
        , stmtQuery     = withStmt' stmt
        }

execute' :: O.Statement -> [PersistValue] -> IO Int64
execute' query vals = fmap fromInteger $ O.execute query $ map (HSV.toSql . P) vals

withStmt' :: MonadIO m
          => O.Statement
          -> [PersistValue]
          -> Acquire (ConduitT () [PersistValue] m ())
withStmt' stmt vals = do
#if DEBUG
    liftIO $ putStrLn $ "withStmt': vals: " ++ show vals
#endif
    result <- mkAcquire openS closeS
    return $ pull result 
    --bracketP openS closeS pull
  where
    openS       = execute' stmt vals >> return ()
    closeS _    = O.finish stmt
    pull x      = do
        mr <- liftIO $ O.fetchRow stmt
        maybe (return ()) 
              (\r -> do
#if DEBUG
                    liftIO $ putStrLn $ "withStmt': yield: " ++ show r
                    liftIO $ putStrLn $ "withStmt': yield2: " ++ show (map (unP . HSV.fromSql) r)
#endif
                    yield (map (unP . HSV.fromSql) r)
                    pull x
              )
              mr

-- | Information required to connect to an ODBC database
-- using @persistent@'s generic facilities.  These values are the
-- same that are given to 'withODBCPool'.
data OdbcConf = OdbcConf
    { odbcConnStr  :: ConnectionString
      -- ^ The connection string.
    , odbcPoolSize :: Int
      -- ^ How many connections should be held on the connection pool.
    , odbcDbtype :: String
    }

instance PersistConfig OdbcConf where
    type PersistConfigBackend OdbcConf = SqlPersistT
    type PersistConfigPool OdbcConf = ConnectionPool
    createPoolConfig (OdbcConf cs size dbtype) = runNoLoggingT $ createODBCPool (read dbtype) cs size
    runPool _ = runSqlPool
    loadConfig (Object o) = do
        cstr    <- o .: "connStr"
        pool    <- o .: "poolsize"
        dbtype  <- o .: "dbtype"
        return $ OdbcConf cstr pool dbtype
    loadConfig _ = mzero

    applyEnv c0 = return c0


-- | Avoid orphan instances.
newtype P = P { unP :: PersistValue } deriving Show

instance DC.Convertible P HSV.SqlValue where
    safeConvert (P (PersistText t))             = Right $ HSV.toSql t
    safeConvert (P (PersistByteString bs))      = Right $ HSV.toSql bs
    safeConvert (P (PersistInt64 i))            = Right $ HSV.toSql i
    safeConvert (P (PersistRational r))         = Right $ HSV.toSql (fromRational r::Double)
    safeConvert (P (PersistDouble d))           = Right $ HSV.toSql d
    safeConvert (P (PersistBool b))             = Right $ HSV.SqlInteger (if b then 1 else 0)
    safeConvert (P (PersistDay d))              = Right $ HSV.toSql d
    safeConvert (P (PersistTimeOfDay t))        = Right $ HSV.toSql t
    safeConvert (P (PersistUTCTime t))          = Right $ HSV.toSql t
--    safeConvert (P (PersistZonedTime (ZT t)))   = Right $ HSV.toSql t
    safeConvert (P PersistNull)                 = Right HSV.SqlNull
    safeConvert (P (PersistList l))             = Right $ HSV.toSql $ listToJSON l
    safeConvert (P (PersistMap m))              = Right $ HSV.toSql $ mapToJSON m
    safeConvert p@(P (PersistObjectId _))       = Left DC.ConvertError 
        { DC.convSourceValue   = show p
        , DC.convSourceType    = "P (PersistValue)" 
        , DC.convDestType      = "SqlValue"
        , DC.convErrorMessage  = "Refusing to serialize a PersistObjectId to an ODBC value" }
    safeConvert xs                              = error $ "unhandled safeConvert xs=" ++ show xs


-- FIXME: check if those are correct and complete.
instance DC.Convertible HSV.SqlValue P where 
    safeConvert (HSV.SqlString s)        = Right $ P $ PersistText $ T.pack s
    safeConvert (HSV.SqlByteString bs)   = Right $ P $ PersistByteString bs
    safeConvert v@(HSV.SqlWord32 _)      = Left DC.ConvertError
        { DC.convSourceValue   = show v
        , DC.convSourceType    = "SqlValue"
        , DC.convDestType      = "P (PersistValue)"
        , DC.convErrorMessage  = "There is no conversion from SqlWord32 to PersistValue" }
    safeConvert v@(HSV.SqlWord64 _)      = Left DC.ConvertError
        { DC.convSourceValue   = show v
        , DC.convSourceType    = "SqlValue"
        , DC.convDestType      = "P (PersistValue)"
        , DC.convErrorMessage  = "There is no conversion from SqlWord64 to PersistValue" }
    safeConvert (HSV.SqlInt32 i)         = Right $ P $ PersistInt64 $ fromIntegral i
    safeConvert (HSV.SqlInt64 i)         = Right $ P $ PersistInt64 i
    safeConvert (HSV.SqlInteger i)       = Right $ P $ PersistInt64 $ fromIntegral i
    safeConvert (HSV.SqlChar c)          = Right $ P $ charChk c
    safeConvert (HSV.SqlBool b)          = Right $ P $ PersistBool b
    safeConvert (HSV.SqlDouble d)        = Right $ P $ PersistDouble d
    safeConvert (HSV.SqlRational r)      = Right $ P $ PersistRational r
    safeConvert (HSV.SqlLocalDate d)     = Right $ P $ PersistDay d
    safeConvert (HSV.SqlLocalTimeOfDay t)= Right $ P $ PersistTimeOfDay t
    safeConvert (HSV.SqlZonedLocalTimeOfDay td _) = Right $ P $ PersistTimeOfDay td
    safeConvert (HSV.SqlLocalTime t)     = Right $ P $ PersistUTCTime $ localTimeToUTC utc t
    safeConvert (HSV.SqlZonedTime zt)    = Right $ P $ PersistUTCTime $ localTimeToUTC utc (zonedTimeToLocalTime zt)
    safeConvert (HSV.SqlUTCTime t)       = Right $ P $ PersistUTCTime t
    safeConvert (HSV.SqlDiffTime ndt)    = Right $ P $ PersistDouble $ fromRational $ toRational ndt
    safeConvert (HSV.SqlPOSIXTime pt)    = Right $ P $ PersistDouble $ fromRational $ toRational pt
    safeConvert (HSV.SqlEpochTime e)     = Right $ P $ PersistInt64 $ fromIntegral e
    safeConvert (HSV.SqlTimeDiff i)      = Right $ P $ PersistInt64 $ fromIntegral i
    safeConvert (HSV.SqlNull)            = Right $ P PersistNull

charChk :: Char -> PersistValue
charChk '\0' = PersistBool False
charChk '\1' = PersistBool True
charChk c = PersistText $ T.singleton c
