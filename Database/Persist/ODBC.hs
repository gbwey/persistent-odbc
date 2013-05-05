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
    ) where

import qualified Control.Exception as E -- (catch, SomeException)
import Database.Persist.Sql
{-
 - hiding (Entity (..))
import Database.Persist.Store
import Database.Persist.GenericSql hiding (Key)
import Database.Persist.GenericSql.Internal
import Database.Persist.EntityDef
-}
import Data.Time(ZonedTime(..), LocalTime(..), Day(..))

import qualified Database.HDBC.ODBC as O
import qualified Database.HDBC as O
import qualified Database.HDBC.SqlValue as HSV
import qualified Data.Convertible as DC

import Control.Monad.IO.Class (MonadIO (..))
import Data.List (intercalate)
import Data.IORef(newIORef)
import qualified Data.Map as Map
import Data.Conduit(Source, yield, MonadBaseControl, MonadResource, bracketP)

-- import Data.ByteString (ByteString)
-- import qualified Data.ByteString.Char8 as B8
import qualified Data.Text as T
-- import qualified Data.Text.Encoding as T
import Data.Time.LocalTime (localTimeToUTC, utc)
import Data.Text (Text, pack)
import Data.Aeson -- (Object(..), (.:))
import Control.Monad (mzero)
import Data.Int (Int64)

-- | An @HDBC-odbc@ connection string.  A simple example of connection
-- string would be @DSN=hdbctest1@. 
type ConnectionString = String

-- | Create an ODBC connection pool and run the given
-- action.  The pool is properly released after the action
-- finishes using it.  Note that you should not use the given
-- 'ConnectionPool' outside the action since it may be already
-- been released.
withODBCPool :: MonadIO m
             => ConnectionString
             -- ^ Connection string to the database.
             -> Int
             -- ^ Number of connections to be kept open in
             -- the pool.
             -> (ConnectionPool -> m a)
             -- ^ Action to be executed that uses the
             -- connection pool.
             -> m a
withODBCPool ci = withSqlPool $ open' ci


-- | Create an ODBC connection pool.  Note that it's your
-- responsability to properly close the connection pool when
-- unneeded.  Use 'withODBCPool' for an automatic resource
-- control.
createODBCPool :: MonadIO m
               => ConnectionString
               -- ^ Connection string to the database.
               -> Int
               -- ^ Number of connections to be kept open
               -- in the pool.
               -> m ConnectionPool
createODBCPool ci = createSqlPool $ open' ci


-- | Same as 'withODBCPool', but instead of opening a pool
-- of connections, only one connection is opened.
withODBCConn :: (MonadIO m, MonadBaseControl IO m)
             => ConnectionString -> (Connection -> m a) -> m a
withODBCConn = withSqlConn . open'

open' :: ConnectionString -> IO Connection
open' cstr = do
    O.connectODBC cstr >>= openSimpleConn

-- | Generate a persistent 'Connection' from an odbc 'O.Connection'
openSimpleConn :: O.Connection -> IO Connection
openSimpleConn conn = do
    smap <- newIORef $ Map.empty
    return Connection
        { connPrepare       = prepare' conn
        , connStmtMap       = smap
        , connInsertSql     = insertSql'
        , connClose         = O.disconnect conn
        , connMigrateSql    = migrate'
        , connBegin         = const 
                     $ E.catch (O.commit conn) (\(_ :: E.SomeException) -> return ())  
            -- there is no nested transactions.
            -- Transaction begining means that previous commited
        , connCommit        = const $ O.commit   conn
        , connRollback      = const $ O.rollback conn
        , connEscapeName    = escape
        , connNoLimit       = "" -- "LIMIT ALL"
        , connRDBMS         = "odbc" -- ?
        }

prepare' :: O.Connection -> Text -> IO Statement
prepare' conn sql = do
#if DEBUG
    liftIO $ "Database.Persist.ODBC.prepare': sql = " ++ T.unpack sql
#endif
    stmt <- O.prepare conn $ T.unpack sql
    
    return Statement
        { stmtFinalize  = O.finish stmt
        , stmtReset     = return () -- rollback conn ?
        , stmtExecute   = execute' stmt
        , stmtQuery     = withStmt' stmt
        }

insertSql' :: DBName -> [DBName] -> DBName -> InsertSqlResult
insertSql' t cols id' = ISRSingle $ pack $ concat
    [ "INSERT INTO "
    , T.unpack $ escape t
    , "("
    , intercalate "," $ map (T.unpack . escape) cols
    , ") VALUES("
    , intercalate "," (map (const "?") cols)
    , ") RETURNING "
    , T.unpack $ escape id'
    ]

execute' :: O.Statement -> [PersistValue] -> IO Int64
execute' query vals = fmap fromInteger $ O.execute query $ map (HSV.toSql . P) vals

withStmt' :: MonadResource m
          => O.Statement
          -> [PersistValue]
          -> Source m [PersistValue]
withStmt' stmt vals = do
    bracketP openS closeS pull
  where
    openS       = execute' stmt vals >> return ()
    closeS _    = O.finish stmt
    pull x      = do
        mr <- liftIO $ O.fetchRow stmt
        maybe (return ()) 
              (\r -> yield (map (unP . HSV.fromSql) r) >> pull x)
              mr

escape :: DBName -> Text
escape (DBName s) =
    T.pack $ '"' : go (T.unpack s) ++ "\""
  where
    go "" = ""
    go ('"':xs) = "\"\"" ++ go xs
    go (x:xs) = x : go xs

-- | Information required to connect to a PostgreSQL database
-- using @persistent@'s generic facilities.  These values are the
-- same that are given to 'withODBCPool'.
data OdbcConf = OdbcConf
    { odbcConnStr  :: ConnectionString
      -- ^ The connection string.
    , odbcPoolSize :: Int
      -- ^ How many connections should be held on the connection pool.
    }

instance PersistConfig OdbcConf where
    type PersistConfigBackend OdbcConf = SqlPersistT
    type PersistConfigPool OdbcConf = ConnectionPool
    createPoolConfig (OdbcConf cs size) = createODBCPool cs size
    runPool _ = runSqlPool
    loadConfig (Object o) = do
        cstr    <- o .: "connStr"
        pool    <- o .: "poolsize"
        return $ OdbcConf cstr pool
    loadConfig _ = mzero

    applyEnv c0 = return c0


-- | Avoid orphan instances.
newtype P = P { unP :: PersistValue } deriving Show

instance DC.Convertible P HSV.SqlValue where
    safeConvert (P (PersistText t))             = Right $ HSV.toSql t
    safeConvert (P (PersistByteString bs))      = Right $ HSV.toSql bs
    safeConvert (P (PersistInt64 i))            = Right $ HSV.toSql i
    safeConvert (P (PersistRational r))         = Right $ HSV.toSql r
    safeConvert (P (PersistDouble d))           = Right $ HSV.toSql d
    safeConvert (P (PersistBool b))             = Right $ HSV.toSql b
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
    safeConvert (HSV.SqlChar c)          = Right $ P $ PersistText $ T.singleton c
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

migrate' :: [EntityDef a]
         -> (Text -> IO Statement)
         => EntityDef SqlType
         -> IO (Either [Text] [(Bool, Text)])
migrate' _ _ _ = error "migrate is not implemented for ODBC"

{-
    = fmap (fmap $ map showAlterDb) $ do
    let name = entityDB $ entityDef val
    old <- getColumns getter $ entityDef val
    case partitionEithers old of
        ([], old'') -> do
            let old' = partitionEithers old''
            let new = second (map udToPair) $ mkColumns allDefs val
            if null old
                then do
                    let addTable = AddTable $ concat
                            -- Lower case e: see Database.Persistent.GenericSql.Migration
                            [ "CREATe TABLE "
                            , T.unpack $ escape name
                            , "("
                            , T.unpack $ escape $ entityID $ entityDef val
                            , " SERIAL PRIMARY KEY UNIQUE"
                            , concatMap (\x -> ',' : showColumn x) $ fst new
                            , ")"
                            ]
                    let uniques = flip concatMap (snd new) $ \(uname, ucols) ->
                            [AlterTable name $ AddUniqueConstraint uname ucols]
                        references = mapMaybe (getAddReference name) $ fst new
                    return $ Right $ addTable : uniques ++ references
                else do
                    let (acs, ats) = getAlters new old'
                    let acs' = map (AlterColumn name) acs
                    let ats' = map (AlterTable name) ats
                    return $ Right $ acs' ++ ats'
        (errs, _) -> return $ Left errs

data AlterColumn = Type SqlType | IsNull | NotNull | Add Column | Drop
                 | Default String | NoDefault | Update String
                 | AddReference DBName | DropReference DBName
type AlterColumn' = (DBName, AlterColumn)

data AlterTable = AddUniqueConstraint DBName [DBName]
                | DropConstraint DBName

data AlterDB = AddTable Stringmr <- 
             | AlterColumn DBName AlterColumn'
             | AlterTable DBName AlterTable

-- | Returns all of the columns in the given table currently in the database.
getColumns :: (Text -> IO Statement)
           -> EntityDef
           -> IO [Either Text (Either Column (DBName, [DBName]))]
getColumns getter def = do
    stmt <- getter "SELECT column_name,is_nullable,udt_name,column_default FROM information_schema.columns WHERE table_name=? AND column_name <> ?"
    let vals =
            [ PersistText $ unDBName $ entityDB def
            , PersistText $ unDBName $ entityID def
            ]
    cs <- runResourceT $ withStmt stmt vals $$ helper
    stmt' <- getter
        "SELECT constraint_name, column_name FROM information_schema.constraint_column_usage WHERE table_name=? AND column_name <> ? ORDER BY constraint_name, column_name"
    us <- runResourceT $ withStmt stmt' vals $$ helperU
    return $ cs ++ us
  where
    getAll front = do
        x <- CL.head
        case x of
            Nothing -> return $ front []
            Just [PersistText con, PersistText col] ->
                getAll (front . (:) (con, col))
            Just _ -> getAll front -- FIXME error message?
    helperU = do
        rows <- getAll id
        return $ map (Right . Right . (DBName . fst . head &&& map (DBName . snd)))
               $ groupBy ((==) `on` fst) rows
    helper = do
        x <- CL.head
        case x of
            Nothing -> return []
            Just x' -> do
                col <- liftIO $ getColumn getter (entityDB def) x'
                let col' = case col of
                            Left e -> Left e
                            Right c -> Right $ Left c
                cols <- helper
                return $ col' : cols

getAlters :: ([Column], [(DBName, [DBName])])
          -> ([Column], [(DBName, [DBName])])
          -> ([AlterColumn'], [AlterTable])
getAlters (c1, u1) (c2, u2) =
    (getAltersC c1 c2, getAltersU u1 u2)
  where
    getAltersC [] old = map (\x -> (cName x, Drop)) old
    getAltersC (new:news) old =
        let (alters, old') = findAlters new old
         in alters ++ getAltersC news old'

    getAltersU :: [(DBName, [DBName])]
               -> [(DBName, [DBName])]
               -> [AlterTable]
    getAltersU [] old = map DropConstraint $ filter (not . isManual) $ map fst old
    getAltersU ((name, cols):news) old =
        case lookup name old of
            Nothing -> AddUniqueConstraint name cols : getAltersU news old
            Just ocols ->
                let old' = filter (\(x, _) -> x /= name) old
                 in if sort cols == sort ocols
                        then getAltersU news old'
                        else  DropConstraint name
                            : AddUniqueConstraint name cols
                            : getAltersU news old'

    -- Don't drop constraints which were manually added.
    isManual (DBName x) = "__manual_" `T.isPrefixOf` x

getColumn :: (Text -> IO Statement)
          -> DBName -> [PersistValue]
          -> IO (Either Text Column)
getColumn getter tname [PersistText x, PersistText y, PersistText z, d] =
    case d' of
        Left s -> return $ Left s
        Right d'' ->
            case getType z of
                Left s -> return $ Left s
                Right t -> do
                    let cname = DBName x
                    ref <- getRef cname
                    return $ Right $ Column cname (y == "YES") t d'' Nothing ref
  where
    getRef cname = do
        let sql = pack $ concat
                [ "SELECT COUNT(*) FROM "
                , "information_schema.table_constraints "
                , "WHERE table_name=? "
                , "AND constraint_type='FOREIGN KEY' "
                , "AND constraint_name=?"
                ]
        let ref = refName tname cname
        stmt <- getter sql
        runResourceT $ withStmt stmt
                     [ PersistText $ unDBName tname
                     , PersistText $ unDBName ref
                     ] $$ do
            Just [PersistInt64 i] <- CL.head
            return $ if i == 0 then Nothing else Just (DBName "", ref)
    d' = case d of
            PersistNull   -> Right Nothing
            PersistText t -> Right $ Just t
            _ -> Left $ pack $ "Invalid default column: " ++ show d
    getType "int4"        = Right $ SqlInt32
    getType "int8"        = Right $ SqlInt64
    getType "varchar"     = Right $ SqlString
    getType "date"        = Right $ SqlDay
    getType "bool"        = Right $ SqlBool
    getType "timestamp"   = Right $ SqlDayTime
    getType "timestamptz" = Right $ SqlDayTimeZoned
    getType "float4"      = Right $ SqlReal
    getType "float8"      = Right $ SqlReal
    getType "bytea"       = Right $ SqlBlob
    getType "time"        = Right $ SqlTime
    getType a             = Right $ SqlOther a
getColumn _ _ x =
    return $ Left $ pack $ "Invalid result from information_schema: " ++ show x

findAlters :: Column -> [Column] -> ([AlterColumn'], [Column])
findAlters col@(Column name isNull type_ def _maxLen ref) cols =
    case filter (\c -> cName c == name) cols of
        [] -> ([(name, Add col)], cols)
        Column _ isNull' type_' def' _maxLen' ref':_ ->
            let refDrop Nothing = []
                refDrop (Just (_, cname)) = [(name, DropReference cname)]
                refAdd Nothing = []
                refAdd (Just (tname, _)) = [(name, AddReference tname)]
                modRef =
                    if fmap snd ref == fmap snd ref'
                        then []
                        else refDrop ref' ++ refAdd ref
                modNull = case (isNull, isNull') of
                            (True, False) -> [(name, IsNull)]
                            (False, True) ->
                                let up = case def of
                                            Nothing -> id
                                            Just s -> (:) (name, Update $ T.unpack s)
                                 in up [(name, NotNull)]
                            _ -> []
                modType = if type_ == type_' then [] else [(name, Type type_)]
                modDef =
                    if def == def'
                        then []
                        else case def of
                                Nothing -> [(name, NoDefault)]
                                Just s -> [(name, Default $ T.unpack s)]
             in (modRef ++ modDef ++ modNull ++ modType,
                 filter (\c -> cName c /= name) cols)

-- | Get the references to be added to a table for the given column.
getAddReference :: DBName -> Column -> Maybe AlterDB
getAddReference table (Column n _nu _t _def _maxLen ref) =
    case ref of
        Nothing -> Nothing
        Just (s, _) -> Just $ AlterColumn table (n, AddReference s)

showColumn :: Column -> String
showColumn (Column n nu t def _maxLen ref) = concat
    [ T.unpack $ escape n
    , " "
    , showSqlType t
    , " "
    , if nu then "NULL" else "NOT NULL"
    , case def of
        Nothing -> ""
        Just s -> " DEFAULT " ++ T.unpack s
    ]

showSqlType :: SqlType -> String
showSqlType SqlString = "VARCHAR"
showSqlType SqlInt32 = "INT4"
showSqlType SqlInt64 = "INT8"
showSqlType SqlReal = "DOUBLE PRECISION"
showSqlType SqlDay = "DATE"
showSqlType SqlTime = "TIME"
showSqlType SqlDayTime = "TIMESTAMP"
showSqlType SqlDayTimeZoned = "TIMESTAMP WITH TIME ZONE"
showSqlType SqlBlob = "BYTEA"
showSqlType SqlBool = "BOOLEAN"
showSqlType (SqlOther t) = T.unpack t

showAlterDb :: AlterDB -> (Bool, Text)
showAlterDb (AddTable s) = (False, pack s)
showAlterDb (AlterColumn t (c, ac)) =
    (isUnsafe ac, pack $ showAlter t (c, ac))
  where
    isUnsafe Drop = True
    isUnsafe _ = False
showAlterDb (AlterTable t at) = (False, pack $ showAlterTable t at)

showAlterTable :: DBName -> AlterTable -> String
showAlterTable table (AddUniqueConstraint cname cols) = concat
    [ "ALTER TABLE "
    , T.unpack $ escape table
    , " ADD CONSTRAINT "
    , T.unpack $ escape cname
    , " UNIQUE("
    , intercalate "," $ map (T.unpack . escape) cols
    , ")"
    ]
showAlterTable table (DropConstraint cname) = concat
    [ "ALTER TABLE "
    , T.unpack $ escape table
    , " DROP CONSTRAINT "
    , T.unpack $ escape cname
    ]

showAlter :: DBName -> AlterColumn' -> String
showAlter table (n, Type t) =
    concat
        [ "ALTER TABLE "
        , T.unpack $ escape table
        , " ALTER COLUMN "
        , T.unpack $ escape n
        , " TYPE "
        , showSqlType t
        ]
showAlter table (n, IsNull) =
    concat
        [ "ALTER TABLE "
        , T.unpack $ escape table
        , " ALTER COLUMN "
        , T.unpack $ escape n
        , " DROP NOT NULL"
        ]
showAlter table (n, NotNull) =
    concat
        [ "ALTER TABLE "
        , T.unpack $ escape table
        , " ALTER COLUMN "
        , T.unpack $ escape n
        , " SET NOT NULL"
        ]
showAlter table (_, Add col) =
    concat
        [ "ALTER TABLE "
        , T.unpack $ escape table
        , " ADD COLUMN "
        , showColumn col
        ]
showAlter table (n, Drop) =
    concat
        [ "ALTER TABLE "
        , T.unpack $ escape table
        , " DROP COLUMN "
        , T.unpack $ escape n
        ]
showAlter table (n, Default s) =
    concat
        [ "ALTER TABLE "
        , T.unpack $ escape table
        , " ALTER COLUMN "
        , T.unpack $ escape n
        , " SET DEFAULT "
        , s
        ]
showAlter table (n, NoDefault) = concat
    [ "ALTER TABLE "
    , T.unpack $ escape table
    , " ALTER COLUMN "
    , T.unpack $ escape n
    , " DROP DEFAULT"
    ]
showAlter table (n, Update s) = concat
    [ "UPDATE "
    , T.unpack $ escape table
    , " SET "
    , T.unpack $ escape n
    , "="
    , s
    , " WHERE "
    , T.unpack $ escape n
    , " IS NULL"
    ]
showAlter table (n, AddReference t2) = concat
    [ "ALTER TABLE "
    , T.unpack $ escape table
    , " ADD CONSTRAINT "
    , T.unpack $ escape $ refName table n
    , " FOREIGN KEY("
    , T.unpack $ escape n
    , ") REFERENCES "
    , T.unpack $ escape t2
    ]
showAlter table (_, DropReference cname) = concat
    [ "ALTER TABLE "
    , T.unpack (escape table)
    , " DROP CONSTRAINT "
    , T.unpack $ escape cname
    ]
-}
{-
refName :: DBName -> DBName -> DBName
refName (DBName table) (DBName column) =
    DBName $ T.concat [table, "_", column, "_fkey"]

udToPair :: UniqueDef -> (DBName, [DBName])
udToPair ud = (uniqueDBName ud, map snd $ uniqueFields ud)
-}
