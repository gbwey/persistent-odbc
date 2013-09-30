{-# LANGUAGE EmptyDataDecls    #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses, ScopedTypeVariables #-}
-- | An ODBC backend for persistent.
module Database.Persist.MigratePostgres
    ( getMigrationStrategy 
    ) where

import Database.Persist.Sql
import Control.Monad.IO.Class (MonadIO (..))
import Data.List (intercalate)
import qualified Data.Text as T
import Data.Text (pack,Text)

import Data.Either (partitionEithers)
import Control.Arrow
import Data.List (sort, groupBy)
import Data.Function (on)
import Data.Conduit
import qualified Data.Conduit.List as CL
import Data.Maybe (mapMaybe)

import qualified Data.Text.Encoding as TE

import Database.Persist.ODBCTypes
import Debug.Trace

getMigrationStrategy :: DBType -> MigrationStrategy
getMigrationStrategy dbtype@Postgres {} = 
     MigrationStrategy
                          { dbmsLimitOffset=decorateSQLWithLimitOffset "LIMIT ALL" 
                           ,dbmsMigrate=migrate' 
                           ,dbmsInsertSql=insertSql' 
                           ,dbmsEscape=escape 
                           ,dbmsType=dbtype
                          } 
getMigrationStrategy dbtype = error $ "Postgres: calling with invalid dbtype " ++ show dbtype
                     
migrate' :: [EntityDef a]
         -> (Text -> IO Statement)
         => EntityDef SqlType
         -> IO (Either [Text] [(Bool, Text)])
migrate' allDefs getter val = fmap (fmap $ map showAlterDb) $ do
    let name = entityDB val
    old <- getColumns getter val
    case partitionEithers old of
        ([], old'') -> do
            let old' = partitionEithers old''
            let new = first (filter $ not . safeToRemove val . cName)
                    $ second (map udToPair)
                    $ mkColumns allDefs val
            if null old
                then do
                    let addTable = AddTable $ concat
                            -- Lower case e: see Database.Persistent.GenericSql.Migration
                            [ "CREATE TABLE "
                            , T.unpack $ escape name
                            , "("
                            , T.unpack $ escape $ entityID val
                            , " SERIAL PRIMARY KEY UNIQUE"
                            , concatMap (\x -> ',' : showColumn x) $ fst new
                            , ")"
                            ]
                    let uniques = flip concatMap (snd new) $ \(uname, ucols) ->
                            [AlterTable name $ AddUniqueConstraint uname ucols]
                        references = mapMaybe (getAddReference name) $ fst new
                    return $ Right $ addTable : uniques ++ references
                else do
                    let (acs, ats) = getAlters val new old'
                    let acs' = map (AlterColumn name) acs
                    let ats' = map (AlterTable name) ats
                    return $ Right $ acs' ++ ats'
        (errs, _) -> return $ Left errs

type SafeToRemove = Bool

data AlterColumn = Type SqlType | IsNull | NotNull | Add' Column | Drop SafeToRemove
                 | Default String | NoDefault | Update' String
                 | AddReference DBName | DropReference DBName
type AlterColumn' = (DBName, AlterColumn)

data AlterTable = AddUniqueConstraint DBName [DBName]
                | DropConstraint DBName

data AlterDB = AddTable String
             | AlterColumn DBName AlterColumn'
             | AlterTable DBName AlterTable

-- | Returns all of the columns in the given table currently in the database.
getColumns :: (Text -> IO Statement)
           -> EntityDef a
           -> IO [Either Text (Either Column (DBName, [DBName]))]
getColumns getter def = do
    let sqlv=concat ["SELECT "
                          ,"column_name "
                          ,",is_nullable "
                          ,",udt_name "
                          ,",column_default "
                          ,",numeric_precision "
                          ,",numeric_scale "
                          ,"FROM information_schema.columns "
                          ,"WHERE table_catalog=current_database() "
                          ,"AND table_name=? "
                          ,"AND column_name <> ?"]
  
    stmt <- getter $ pack sqlv
    let vals =
            [ PersistText $ unDBName $ entityDB def
            , PersistText $ unDBName $ entityID def
            ]
    cs <- runResourceT $ stmtQuery stmt vals $$ helper
    let sqlc=concat ["SELECT "
                          ,"constraint_name "
                          ,",column_name "
                          ,"FROM information_schema.constraint_column_usage "
                          ,"WHERE table_catalog=current_database() "
                          ,"AND table_name=? "
                          ,"AND column_name <> ? "
                          ,"ORDER BY constraint_name, column_name"]

    stmt' <- getter $ pack sqlc
        
    us <- runResourceT $ stmtQuery stmt' vals $$ helperU
    return $ cs ++ us
  where
    getAll front = do
        x <- CL.head
        case x of
            Nothing -> return $ front []
            Just [PersistText con, PersistText col] ->
                getAll (front . (:) (con, col))
            Just [PersistByteString con, PersistByteString col] -> do
                getAll (front . (:) (TE.decodeUtf8 con, TE.decodeUtf8 col)) 
            Just xx -> error ("oops: unexpected datatype returned odbc postgres  xx="++show xx) -- $ getAll front -- FIXME error message?
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

-- | Check if a column name is listed as the "safe to remove" in the entity
-- list.
safeToRemove :: EntityDef a -> DBName -> Bool
safeToRemove def (DBName colName)
    = any (elem "SafeToRemove" . fieldAttrs)
    $ filter ((== (DBName colName)) . fieldDB)
    $ entityFields def

getAlters :: EntityDef a
          -> ([Column], [(DBName, [DBName])])
          -> ([Column], [(DBName, [DBName])])
          -> ([AlterColumn'], [AlterTable])
getAlters def (c1, u1) (c2, u2) =
    (getAltersC c1 c2, getAltersU u1 u2)
  where
    getAltersC [] old = map (\x -> (cName x, Drop $ safeToRemove def $ cName x)) old
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
getColumn getter tname [PersistByteString x, PersistByteString y, PersistByteString z, d, npre, nscl] = do
    case d' of
        Left s -> return $ Left s
        Right d'' ->
            case getType (TE.decodeUtf8 z) of
                Left s -> return $ Left s
                Right t -> do
                    let cname = DBName $ TE.decodeUtf8 x
                    ref <- getRef cname
                    return $ Right Column
                        { cName = cname
                        , cNull = y == "YES"
                        , cSqlType = t
                        , cDefault = d''
                        , cDefaultConstraintName = Nothing
                        , cMaxLen = Nothing
                        , cReference = ref
                        }
  where
    getRef cname = do
        let sql = concat
                [ "SELECT COUNT(*) FROM "
                , "information_schema.table_constraints "
                , "WHERE table_catalog=current_database()"
                , "AND table_name=?"
                , "AND constraint_type='FOREIGN KEY' "
                , "AND constraint_name=?"
                ]
        let ref = refName tname cname
        stmt <- getter $ pack sql
        runResourceT $ stmtQuery stmt
                     [ PersistText $ unDBName tname
                     , PersistText $ unDBName ref
                     ] $$ do
            Just [PersistInt64 i] <- CL.head
            return $ if i == 0 then Nothing else Just (DBName "", ref)
    d' = case d of
            PersistNull   -> Right Nothing
            PersistText t -> Right $ Just t
            PersistByteString bs -> Right $ Just $ TE.decodeUtf8 bs
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
    getType "numeric"     = getNumeric npre nscl
    getType a             = Right $ SqlOther a

    getNumeric (PersistInt64 a) (PersistInt64 b) = Right $ SqlNumeric (fromIntegral a) (fromIntegral b)
    getNumeric a b = Left $ pack $ "Can not get numeric field precision, got: " ++ show a ++ " and " ++ show b ++ " as precision and scale"
getColumn _ a2 x =
    return $ Left $ pack $ "Invalid result from information_schema: " ++ show x ++ " a2[" ++ show a2 ++ "]"

findAlters :: Column -> [Column] -> ([AlterColumn'], [Column])
findAlters col@(Column name isNull sqltype def defConstraintName _maxLen ref) cols =
    case filter (\c -> cName c == name) cols of
        [] -> ([(name, Add' col)], cols)
        Column _ isNull' sqltype' def' defConstraintName' _maxLen' ref':_ ->
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
                                            Just s -> (:) (name, Update' $ T.unpack s)
                                 in up [(name, NotNull)]
                            _ -> []
                modType = if sqltype == sqltype' then [] else [(name, Type sqltype)]
                modDef = --trace ("findAlters col=" ++ show col ++ " def=" ++ show def ++ " def'=" ++ show def') $
                    if cmpdef def def'
                        then []
                        else case def of
                                Nothing -> [(name, NoDefault)]
                                Just s -> [(name, Default $ T.unpack s)]
             in (modRef ++ modDef ++ modNull ++ modType,
                 filter (\c -> cName c /= name) cols)

cmpdef::Maybe Text -> Maybe Text -> Bool
cmpdef Nothing Nothing = True
cmpdef (Just def) (Just def') | def==def' = True
                              | otherwise = 
        let xs=T.dropWhile (/=':') $ T.reverse def'
            ys=T.drop 2 xs
            zs=T.reverse ys
        in trace ("cmpdef xs="++show xs++" ys="++show ys++" zs="++show zs) $ def==zs
cmpdef _ _ = False

-- | Get the references to be added to a table for the given column.
getAddReference :: DBName -> Column -> Maybe AlterDB
getAddReference table (Column n _nu _ _def _defConstraintName _maxLen ref) =
    case ref of
        Nothing -> Nothing
        Just (s, _) -> Just $ AlterColumn table (n, AddReference s)

showColumn :: Column -> String
showColumn c@(Column n nu sqlType def defConstraintName _maxLen _ref) = concat
    [ T.unpack $ escape n
    , " "
    , showSqlType sqlType _maxLen
    , " "
    , if nu then "NULL" else "NOT NULL"
    , case def of
        Nothing -> ""
        Just s -> " DEFAULT " ++ T.unpack s
    ]

showSqlType :: SqlType -> Maybe Integer -> String
showSqlType SqlString Nothing = "VARCHAR"
showSqlType SqlString (Just len) = "VARCHAR(" ++ show len ++ ")"
showSqlType SqlInt32 _ = "INT4"
showSqlType SqlInt64 _ = "INT8"
showSqlType SqlReal _ = "DOUBLE PRECISION"
showSqlType (SqlNumeric s prec) _ = "NUMERIC(" ++ show s ++ "," ++ show prec ++ ")"
showSqlType SqlDay _ = "DATE"
showSqlType SqlTime _ = "TIME"
showSqlType SqlDayTime _ = "TIMESTAMP"
showSqlType SqlDayTimeZoned _ = "TIMESTAMP WITH TIME ZONE"
showSqlType SqlBlob _ = "BYTEA"
showSqlType SqlBool _ = "BOOLEAN"
showSqlType (SqlOther t) _ = T.unpack t

showAlterDb :: AlterDB -> (Bool, Text)
showAlterDb (AddTable s) = (False, pack s)
showAlterDb (AlterColumn t (c, ac)) =
    (isUnsafe ac, pack $ showAlter t (c, ac))
  where
    isUnsafe (Drop safeToRemove) = not safeToRemove
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
        , showSqlType t Nothing
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
showAlter table (_, Add' col) =
    concat
        [ "ALTER TABLE "
        , T.unpack $ escape table
        , " ADD COLUMN "
        , showColumn col
        ]
showAlter table (n, Drop _) =
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
showAlter table (n, Update' s) = concat
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

escape :: DBName -> Text
escape (DBName s) =
    T.pack $ '"' : go (T.unpack s) ++ "\""
  where
    go "" = ""
    go ('"':xs) = "\"\"" ++ go xs
    go (x:xs) = x : go xs


refName :: DBName -> DBName -> DBName
refName (DBName table) (DBName column) =
    DBName $ T.concat [table, "_", column, "_fkey"]

udToPair :: UniqueDef -> (DBName, [DBName])
udToPair ud = (uniqueDBName ud, map snd $ uniqueFields ud)

insertSql' :: DBName -> [FieldDef SqlType] -> DBName -> [PersistValue] -> InsertSqlResult
insertSql' t cols id' _ = ISRSingle $ pack $ concat
    [ "INSERT INTO "
    , T.unpack $ escape t
    , "("
    , intercalate "," $ map (T.unpack . escape . fieldDB) cols
    , ") VALUES("
    , intercalate "," (map (const "?") cols)
    , ") RETURNING "
    , T.unpack $ escape id'
    ]
