-- add foreign key support??
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE CPP #-}
-- | A Sqlite backend for @persistent@.
module Database.Persist.MigrateSqlite
    ( getMigrationStrategy 
    ) where

import Data.List (intercalate)
import Data.Text (Text, pack)
import Data.Conduit (connect)
import qualified Data.Conduit.List as CL
import qualified Data.Text as T
import qualified Data.Text.Encoding as T

import Database.Persist.Sql
import Database.Persist.ODBCTypes
import Data.Acquire (with)
import Data.Monoid ((<>))

getMigrationStrategy :: DBType -> MigrationStrategy
getMigrationStrategy dbtype@Sqlite { sqlite3619 = _fksupport } = 
     MigrationStrategy
                          { dbmsLimitOffset=decorateSQLWithLimitOffset "LIMIT -1" 
                           ,dbmsMigrate=migrate'
                           ,dbmsInsertSql=insertSql'
                           ,dbmsEscape=escape
                           ,dbmsType=dbtype
                          }
getMigrationStrategy dbtype = error $ "Sqlite: calling with invalid dbtype " ++ show dbtype

insertSql' :: EntityDef -> [PersistValue] -> InsertSqlResult
insertSql' ent vals =
  case entityPrimary ent of
    Just _ -> 
      ISRManyKeys sql vals
        where sql = pack $ concat
                [ "INSERT INTO "
                , escape' $ entityDB ent
                , "("
                , intercalate "," $ map (escape' . fieldDB) $ entityFields ent
                , ") VALUES("
                , intercalate "," (map (const "?") $ entityFields ent)
                , ")"
                ]
    Nothing -> ISRInsertGet (pack ins) sel
      where
        sel = "SELECT last_insert_rowid()"
        ins = concat
            [ "INSERT INTO "
            , escape' $ entityDB ent
            , "("
            , intercalate "," $ map (escape' . fieldDB) $ entityFields ent
            , ") VALUES("
            , intercalate "," (map (const "?") $ entityFields ent)
            , ")"
            ]

showSqlType :: SqlType -> Text
showSqlType SqlString = "VARCHAR"
showSqlType SqlInt32 = "INTEGER"
showSqlType SqlInt64 = "INTEGER"
showSqlType SqlReal = "REAL"
showSqlType (SqlNumeric precision scale) = pack $ "NUMERIC(" ++ show precision ++ "," ++ show scale ++ ")"
showSqlType SqlDay = "DATE"
showSqlType SqlTime = "TIME"
--showSqlType SqlDayTimeZoned = "TIMESTAMP"
showSqlType SqlDayTime = "TIMESTAMP"
showSqlType SqlBlob = "BLOB"
showSqlType SqlBool = "BOOLEAN"
showSqlType (SqlOther t) = t

migrate' :: [EntityDef]
         -> (Text -> IO Statement)
         -> EntityDef
         -> IO (Either [Text] [(Bool, Text)])
migrate' allDefs getter val = do
    let (cols, uniqs, _fdefs) = mkColumns allDefs val
    let newSql = mkCreateTable False def (filter (not . safeToRemove val . cName) cols, uniqs)
    stmt <- getter "SELECT sql FROM sqlite_master WHERE type='table' AND name=?"
    oldSql' <- with (stmtQuery stmt [PersistText $ unDBName table]) (`connect` go)
    case oldSql' of
        Nothing -> return $ Right [(False, newSql)]
        Just oldSql -> do
            if oldSql == newSql
                then return $ Right []
                else do
                    sql <- getCopyTable allDefs getter val
                    return $ Right sql
  where
    def = val
    table = entityDB def
    go = do
        x <- CL.head
        case x of
            Nothing -> return Nothing
            Just [PersistText y] -> return $ Just y
            Just [PersistByteString y] -> return $ Just $ T.decodeUtf8 y
            Just y -> error $ "Unexpected result from sqlite_master: " ++ show y

-- | Check if a column name is listed as the "safe to remove" in the entity
-- list.
safeToRemove :: EntityDef -> DBName -> Bool
safeToRemove def (DBName colName)
    = any (elem "SafeToRemove" . fieldAttrs)
    $ filter ((== (DBName colName)) . fieldDB)
    $ entityFields def

getCopyTable :: [EntityDef]
             -> (Text -> IO Statement)
             -> EntityDef
             -> IO [(Bool, Text)]
getCopyTable allDefs getter def = do
    stmt <- getter $ pack $ "PRAGMA table_info(" ++ escape' table ++ ")"
    oldCols' <- with (stmtQuery stmt []) (`connect` getCols)
    let oldCols = map DBName $ filter (/= "id") oldCols' -- need to update for table id attribute ?
    let newCols = filter (not . safeToRemove def) $ map cName cols
    let common = filter (`elem` oldCols) newCols
    let id_ = fieldDB (entityId def)
    return [ (False, tmpSql)
           , (False, copyToTemp $ id_ : common)
           , (common /= filter (not . safeToRemove def) oldCols, dropOld)
           , (False, newSql)
           , (False, copyToFinal $ id_ : newCols)
           , (False, dropTmp)
           ]
  where
    getCols = do
        x <- CL.head
        case x of
            Nothing -> return []
            Just (_:PersistText name:_) -> do
                names <- getCols
                return $ name : names
            Just y -> error $ "Invalid result from PRAGMA table_info: " ++ show y
    table = entityDB def
    tableTmp = DBName $ unDBName table <> "_backup"
    (cols, uniqs, _) = mkColumns allDefs def
    cols' = filter (not . safeToRemove def . cName) cols
    newSql = mkCreateTable False def (cols', uniqs)
    tmpSql = mkCreateTable True def { entityDB = tableTmp } (cols', uniqs)
    dropTmp = "DROP TABLE " <> escape tableTmp
    dropOld = "DROP TABLE " <> escape table
    copyToTemp common = T.concat
        [ "INSERT INTO "
        , escape tableTmp
        , "("
        , T.intercalate "," $ map escape common
        , ") SELECT "
        , T.intercalate "," $ map escape common
        , " FROM "
        , escape table
        ]
    copyToFinal newCols = T.concat
        [ "INSERT INTO "
        , escape table
        , " SELECT "
        , T.intercalate "," $ map escape newCols
        , " FROM "
        , escape tableTmp
        ]


escape' :: DBName -> String
escape' = T.unpack . escape

mkCreateTable :: Bool -> EntityDef -> ([Column], [UniqueDef]) -> Text
mkCreateTable isTemp entity (cols, uniqs) =
  case entityPrimary entity of
    Just pdef ->
       T.concat
        [ "CREATE"
        , if isTemp then " TEMP" else ""
        , " TABLE "
        , escape $ entityDB entity
        , "("
        , T.drop 1 $ T.concat $ map sqlColumn cols
        , ", PRIMARY KEY "
        , "("
        , T.intercalate "," $ map (escape . fieldDB) $ compositeFields pdef
        , ")"
        , ")"
        ]
    Nothing -> T.concat
        [ "CREATE"
        , if isTemp then " TEMP" else ""
        , " TABLE "
        , escape $ entityDB entity
        , "("
        , escape $ fieldDB (entityId entity)
        , " "
        , showSqlType $ fieldSqlType $ entityId entity
        ," PRIMARY KEY"
        , mayDefault $ defaultAttribute $ fieldAttrs $ entityId entity
        , T.concat $ map sqlColumn cols
        , T.concat $ map sqlUnique uniqs
        , ")"
        ]
                                        
mayDefault :: Maybe Text -> Text
mayDefault def = case def of
    Nothing -> ""
    Just d -> " DEFAULT " <> d

sqlColumn :: Column -> Text
sqlColumn (Column name isNull typ def _cn _maxLen ref) = T.concat
    [ ","
    , escape name
    , " "
    , showSqlType typ
    , if isNull then " NULL" else " NOT NULL"
    , case def of
        Nothing -> ""
        Just d -> " DEFAULT " `T.append` d
    , case ref of
        Nothing -> ""
        Just (table, _) -> " REFERENCES " `T.append` escape table
    ]

sqlUnique :: UniqueDef -> Text
sqlUnique (UniqueDef _ cname cols _) = T.concat
    [ ",CONSTRAINT "
    , escape cname
    , " UNIQUE ("
    , T.intercalate "," $ map (escape . snd) cols
    , ")"
    ]

escape :: DBName -> Text
escape (DBName s) =
    T.concat [q, T.concatMap go s, q]
  where
    q = T.singleton '"'
    go '"' = "\"\""
    go c = T.singleton c
