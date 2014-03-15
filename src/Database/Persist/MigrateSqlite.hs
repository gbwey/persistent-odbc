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

import Data.Conduit
import qualified Data.Conduit.List as CL
import qualified Data.Text as T
import qualified Data.Text.Encoding as T

import Database.Persist.Sql
import Database.Persist.ODBCTypes

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

insertSql' :: EntityDef SqlType -> [PersistValue] -> InsertSqlResult
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
showSqlType SqlDayTimeZoned = "TIMESTAMP"
showSqlType SqlDayTime = "TIMESTAMP"
showSqlType SqlBlob = "BLOB"
showSqlType SqlBool = "BOOLEAN"
showSqlType (SqlOther t) = t

migrate' :: [EntityDef a]
         -> (Text -> IO Statement)
         -> EntityDef SqlType
         -> IO (Either [Text] [(Bool, Text)])
migrate' allDefs getter val = do
    let (cols, uniqs, _fdefs) = mkColumns allDefs val
    let newSql = mkCreateTable False def (filter (not . safeToRemove val . cName) cols, uniqs)
    stmt <- getter "SELECT sql FROM sqlite_master WHERE type='table' AND name=?"
    oldSql' <- runResourceT
             $ stmtQuery stmt [PersistText $ unDBName table] $$ go
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
safeToRemove :: EntityDef a -> DBName -> Bool
safeToRemove def (DBName colName)
    = any (elem "SafeToRemove" . fieldAttrs)
    $ filter ((== (DBName colName)) . fieldDB)
    $ entityFields def

getCopyTable :: [EntityDef a]
             -> (Text -> IO Statement)
             -> EntityDef SqlType
             -> IO [(Bool, Text)]
getCopyTable allDefs getter val = do
    stmt <- getter $ pack $ "PRAGMA table_info(" ++ escape' table ++ ")"
    oldCols' <- runResourceT $ stmtQuery stmt [] $$ getCols
    let oldCols = map DBName $ filter (/= "id") oldCols' -- need to update for table id attribute ?
    let newCols = filter (not . safeToRemove def) $ map cName cols
    let common = filter (`elem` oldCols) newCols
    let id_ = entityID val
    let ret = case entityPrimary val of
          Just _ -> [ (False, tmpSql)
           , (False, copyToTemp common)
           , (common /= filter (not . safeToRemove def) oldCols, pack dropOld)
           , (False, newSql)
           , (False, copyToFinal newCols)
           , (False, pack dropTmp)
           ]
          Nothing -> [ (False, tmpSql)
           , (False, copyToTemp $ id_ : common)
           , (common /= filter (not . safeToRemove def) oldCols, pack dropOld)
           , (False, newSql)
           , (False, copyToFinal $ id_ : newCols)
           , (False, pack dropTmp)
           ]
    return ret  
  where

    def = val
    getCols = do
        x <- CL.head
        case x of
            Nothing -> return []
            Just (_:PersistText name:_) -> do
                names <- getCols
                return $ name : names
            Just (_:PersistByteString name:_) -> do
                names <- getCols
                return $ T.decodeUtf8 name : names
            Just y -> error $ "Invalid result from PRAGMA table_info: " ++ show y
    table = entityDB def
    tableTmp = DBName $ unDBName table `T.append` "_backup"
    (cols, uniqs, _fdefs) = mkColumns allDefs val
    cols' = filter (not . safeToRemove def . cName) cols
    newSql = mkCreateTable False def (cols', uniqs)
    tmpSql = mkCreateTable True def { entityDB = tableTmp } (cols', uniqs)
    dropTmp = "DROP TABLE " ++ escape' tableTmp
    dropOld = "DROP TABLE " ++ escape' table
    copyToTemp common = pack $ concat
        [ "INSERT INTO "
        , escape' tableTmp
        , "("
        , intercalate "," $ map escape' common
        , ") SELECT "
        , intercalate "," $ map escape' common
        , " FROM "
        , escape' table
        ]
    copyToFinal newCols = pack $ concat
        [ "INSERT INTO "
        , T.unpack $ escape table
        , " SELECT "
        , intercalate "," $ map escape' newCols
        , " FROM "
        , escape' tableTmp
        ]

escape' :: DBName -> String
escape' = T.unpack . escape

mkCreateTable :: Bool -> EntityDef a -> ([Column], [UniqueDef]) -> Text
mkCreateTable isTemp entity (cols, uniqs) = T.concat
    [ "CREATE"
    , if isTemp then " TEMP" else ""
    , " TABLE "
    , escape $ entityDB entity
    , "("
    , T.drop 1 $ T.concat $ map sqlColumn cols
    , ","
    , idx
    , T.concat $ map sqlUnique uniqs
    , ")"
    ] 
    -- gb convert to use text directly
    where idx=case entityPrimary entity of
                  Just pdef -> T.pack $ concat [" PRIMARY KEY (", intercalate "," $ map (T.unpack . escape . snd) $ primaryFields pdef, ")"]
                  Nothing   -> T.pack $ concat [T.unpack $ escape $ entityID entity
                                        ," INTEGER PRIMARY KEY "]
          --cols' = case entityPrimary entity of
          --          Just _ -> drop 1 cols
          --          Nothing -> cols
                                        
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
