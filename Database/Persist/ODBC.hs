{-# LANGUAGE EmptyDataDecls    #-}
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
    ) where

import qualified Control.Exception as E -- (catch, SomeException)
import Database.Persist.Sql
import qualified Database.Persist.MigratePostgres as PG
import qualified Database.Persist.MigrateMySQL as MYSQL
import qualified Database.Persist.MigrateMSSQL as MSSQL
import qualified Database.Persist.MigrateOracle as ORACLE

import Data.Time(ZonedTime(..), LocalTime(..), Day(..))

import qualified Database.HDBC.ODBC as O
import qualified Database.HDBC as O
import qualified Database.HDBC.SqlValue as HSV
import qualified Data.Convertible as DC

import Control.Monad.IO.Class (MonadIO (..))
import Data.IORef(newIORef)
import qualified Data.Map as Map
import qualified Data.Text as T
import Data.Time.LocalTime (localTimeToUTC, utc)
import Data.Text (Text)
import Data.Aeson -- (Object(..), (.:))
import Control.Monad (mzero)
import Data.Int (Int64)
import Data.Conduit
import Database.Persist.ODBCTypes
-- | An @HDBC-odbc@ connection string.  A simple example of connection
-- string would be @DSN=hdbctest1@. 
type ConnectionString = String

-- | Create an ODBC connection pool and run the given
-- action.  The pool is properly released after the action
-- finishes using it.  Note that you should not use the given
-- 'ConnectionPool' outside the action since it may be already
-- been released.
withODBCPool :: MonadIO m
             => DBType 
             -> ConnectionString
             -- ^ Connection string to the database.
             -> Int
             -- ^ Number of connections to be kept open in
             -- the pool.
             -> (ConnectionPool -> m a)
             -- ^ Action to be executed that uses the
             -- connection pool.
             -> m a
withODBCPool dbt ci = withSqlPool $ open' (getMigrationStrategy dbt) ci


-- | Create an ODBC connection pool.  Note that it's your
-- responsability to properly close the connection pool when
-- unneeded.  Use 'withODBCPool' for an automatic resource
-- control.
createODBCPool :: MonadIO m
               => DBType 
               -> ConnectionString
               -- ^ Connection string to the database.
               -> Int
               -- ^ Number of connections to be kept open
               -- in the pool.
               -> m ConnectionPool
createODBCPool dbt ci = createSqlPool $ open' (getMigrationStrategy dbt) ci

-- | Same as 'withODBCPool', but instead of opening a pool
-- of connections, only one connection is opened.
withODBCConn :: (MonadIO m, MonadBaseControl IO m)
             => DBType -> ConnectionString -> (Connection -> m a) -> m a
withODBCConn dbt cs ma = withSqlConn (open' (getMigrationStrategy dbt) cs) ma

open' :: MigrationStrategy -> ConnectionString -> IO Connection
open' mig cstr = do
    O.connectODBC cstr >>= openSimpleConn mig

-- | Generate a persistent 'Connection' from an odbc 'O.Connection'
openSimpleConn :: MigrationStrategy -> O.Connection -> IO Connection
openSimpleConn mig conn = do
    smap <- newIORef $ Map.empty
    return Connection
        { connPrepare       = prepare' conn
        , connStmtMap       = smap
        , connInsertSql     = dbmsInsertSql mig
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
        }

getMigrationStrategy::DBType -> MigrationStrategy
getMigrationStrategy dbtype = 
  case dbtype of
    Postgres -> PG.getMigrationStrategy dbtype
    MySQL -> MYSQL.getMigrationStrategy dbtype
    MSSQL { mssql2012=flag } -> MSSQL.getMigrationStrategy dbtype
    Oracle { oracle12c=flag } -> ORACLE.getMigrationStrategy dbtype


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

withStmt' :: MonadResource m
          => O.Statement
          -> [PersistValue]
          -> Source m [PersistValue]
withStmt' stmt vals = do
#if DEBUG
    liftIO $ putStrLn $ "withStmt': vals: " ++ show vals
#endif
    bracketP openS closeS pull
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

-- | Information required to connect to a PostgreSQL database
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
    createPoolConfig (OdbcConf cs size dbtype) = createODBCPool (read dbtype) cs size
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
    safeConvert (P (PersistZonedTime (ZT t)))   = Right $ HSV.toSql t
    safeConvert (P PersistNull)                 = Right $ HSV.SqlNull
    safeConvert (P (PersistList l))             = Right $ HSV.toSql $ listToJSON l
    safeConvert (P (PersistMap m))              = Right $ HSV.toSql $ mapToJSON m
    safeConvert p@(P (PersistObjectId _))       = Left $ DC.ConvertError 
        { DC.convSourceValue   = show p
        , DC.convSourceType    = "P (PersistValue)" 
        , DC.convDestType      = "SqlValue"
        , DC.convErrorMessage  = "Refusing to serialize a PersistObjectId to an ODBC value" }

-- FIXME: check if those are correct and complete.
instance DC.Convertible HSV.SqlValue P where 
    safeConvert (HSV.SqlString s)        = Right $ P $ PersistText $ T.pack s
    safeConvert (HSV.SqlByteString bs)   = Right $ P $ PersistByteString bs
    safeConvert v@(HSV.SqlWord32 _)      = Left $ DC.ConvertError
        { DC.convSourceValue   = show v
        , DC.convSourceType    = "SqlValue"
        , DC.convDestType      = "P (PersistValue)"
        , DC.convErrorMessage  = "There is no conversion from SqlWord32 to PersistValue" }
    safeConvert v@(HSV.SqlWord64 _)      = Left $ DC.ConvertError
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
    safeConvert (HSV.SqlZonedLocalTimeOfDay td tz)
                    = Right $ P $ PersistZonedTime $ ZT $ ZonedTime (LocalTime (ModifiedJulianDay 0) td) tz
    safeConvert (HSV.SqlLocalTime t)     = Right $ P $ PersistUTCTime $ localTimeToUTC utc t
    safeConvert (HSV.SqlZonedTime zt)    = Right $ P $ PersistZonedTime $ ZT zt
    safeConvert (HSV.SqlUTCTime t)       = Right $ P $ PersistUTCTime t
    safeConvert (HSV.SqlDiffTime ndt)    = Right $ P $ PersistDouble $ fromRational $ toRational ndt
    safeConvert (HSV.SqlPOSIXTime pt)    = Right $ P $ PersistDouble $ fromRational $ toRational pt
    safeConvert (HSV.SqlEpochTime e)     = Right $ P $ PersistInt64 $ fromIntegral e
    safeConvert (HSV.SqlTimeDiff i)      = Right $ P $ PersistInt64 $ fromIntegral i
    safeConvert (HSV.SqlNull)            = Right $ P PersistNull

charChk :: Char -> PersistValue
charChk '\0' = PersistBool True
charChk '\1' = PersistBool False
charChk c = PersistText $ T.singleton c

