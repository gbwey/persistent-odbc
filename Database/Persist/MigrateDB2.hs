{- 
Migrating: ALTER TABLE "table_many" DROP CONSTRAINT "table_many_refone_fkey"
Migrating: ALTER TABLE "table_many" DROP CONSTRAINT "table_many_reftwo_fkey"
Migrating: ALTER TABLE "table_many_many" DROP CONSTRAINT "table_many_many_refone_fkey"
Migrating: ALTER TABLE "table_many_many" DROP CONSTRAINT "table_many_many_refthree_fkey"
Migrating: ALTER TABLE "table_many_many" DROP CONSTRAINT "table_many_many_reftwo_fkey"
-}
{-# LANGUAGE EmptyDataDecls    #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses, ScopedTypeVariables #-}
-- | An ODBC backend for persistent.
module Database.Persist.MigrateDB2
    ( getMigrationStrategy 
    ) where

import Database.Persist.Sql
import Control.Monad.IO.Class (MonadIO (..))
import qualified Data.Text as T
import Data.Text (pack,Text)

import Data.Either (partitionEithers)
import Control.Arrow
import Data.List (intercalate, sort, groupBy)
import Data.Function (on)
import Data.Conduit
import qualified Data.Conduit.List as CL
import Data.Maybe (mapMaybe)
import Data.Monoid ((<>))

import qualified Data.Text.Encoding as TE

import Database.Persist.ODBCTypes
import Debug.Trace

tracex::String -> a -> a
tracex a b = b  -- trace a b

getMigrationStrategy :: DBType -> MigrationStrategy
getMigrationStrategy dbtype@DB2 {} = 
     MigrationStrategy -- gb fix this:what is the limit:otherwise use a custom decorator
                          { dbmsLimitOffset=decorateSQLWithLimitOffset "LIMIT 99999999" 
                           ,dbmsMigrate=migrate' 
                           ,dbmsInsertSql=insertSql' 
                           ,dbmsEscape=escape 
                           ,dbmsType=dbtype
                          } 
getMigrationStrategy dbtype = error $ "DB2: calling with invalid dbtype " ++ show dbtype
                     
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
            let composite = "composite" `elem` entityAttrs val
            if null old
                then do
                    let idtxt = if composite then 
                                  tracex ("found it!!! val=" ++ show val) $ concat [" CONSTRAINT ", T.unpack (escape (pkeyName (entityDB val))), " PRIMARY KEY (", intercalate "," $ map (T.unpack . escape . fieldDB) $ filter (\fd -> null $ fieldManyDB fd) $ entityFields val, ")"]
                                else tracex ("not found val=" ++ show val) $
                                             concat [T.unpack $ escape $ entityID val
                                        , " BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY "]
                    let addTable = AddTable $ concat
                            -- Lower case e: see Database.Persist.Sql.Migration
                            [ "CREATe TABLE "
                            , T.unpack $ escape name
                            , "("
                            , idtxt
                            , if null (fst new) then [] else ","
                            , intercalate "," $ map showColumn $ fst new
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
                          ,"colname column_name "
                          ,",nulls is_nullable "
                          ,",typename "
                          ,",default column_default "
                          ,",length "
                          ,",scale "
                          ,"FROM syscat.columns "
                          ,"WHERE tabschema=current_schema "
                          ,"AND tabname=? "
                          ,"AND colname <> ?"]
  
    stmt <- getter $ pack sqlv
    let vals =
            [ PersistText $ unDBName $ entityDB def
            , PersistText $ unDBName $ entityID def
            , PersistText $ unDBName $ entityDB def
            , PersistText $ unDBName $ entityID def
            ]

    cs <- runResourceT $ stmtQuery stmt vals $$ helper
    let sqlc=concat ["SELECT "
                          ,"a.constname constraint_name "
                          ,",a.colname column_name "
                          ,"FROM SYSCAT.KEYCOLUSE A, SYSCAT.TABCONST B "
                          ,"WHERE A.CONSTNAME=B.CONSTNAME "
                          ,"AND a.tabschema=current_schema "
                          ,"AND b.tabschema=a.tabschema "
                          ,"AND a.tabname=? "
                          ,"AND a.colname <> ? "
                          ,"AND b.type not in ('F','P') "
                          ,"UNION "
                          ,"SELECT "
                          ,"constname constraint_name "
                          ,",trim(pk_colnames) column_name "
                          ,"FROM syscat.references "
                          ,"WHERE tabschema=current_schema "
                          ,"AND reftabname=? "
                          ,"AND trim(pk_colnames) <> ? "
                          ,"ORDER BY constraint_name, column_name"]

    stmt' <- getter $ pack sqlc
        
    us <- runResourceT $ stmtQuery stmt' vals $$ helperU
    tracex ("getColumns: cs="++show cs++"\n\nus="++show us) $ return $ cs ++ us
  where
    getAll front = do
        x <- CL.head
        case x of
            Nothing -> return $ front []
            Just [PersistText con, PersistText col] ->
                getAll (front . (:) (con, col))
            Just [PersistByteString con, PersistByteString col] -> do
                getAll (front . (:) (TE.decodeUtf8 con, TE.decodeUtf8 col)) 
            Just xx -> error ("oops: unexpected datatype returned odbc db2  xx="++show xx) -- $ getAll front -- FIXME error message?
    helperU = do
        rows <- getAll id
        return $ map (Right . Right . (DBName . fst . head &&& map (DBName . snd)))
               $ groupBy ((==) `on` fst) rows
    helper = do
        x <- CL.head
        case x of
            Nothing -> tracex "getColumns helper Nothing!!!" $ return []
            Just x' -> do
                col <- liftIO $ getColumn getter (entityDB def) x'
                let col' = tracex ("getColumns helper: col="++show col) $ case col of
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
            case getType (TE.decodeUtf8 z) d of
                Left s -> return $ Left s
                Right t -> do
                    let cname = DBName $ TE.decodeUtf8 x
                    ref <- getRef cname
                    return $ Right Column
                        { cName = cname
                        , cNull = y == "Y"
                        , cSqlType = t
                        , cDefault = d''
                        , cDefaultConstraintName = Nothing
                        , cMaxLen = Nothing
                        , cReference = ref
                        }
  where -- find the referring table for any fkeys
    getRef cname = do
        let sql=concat ["SELECT "
                       ,"tabname REFERENCED_TABLE_NAME "
                       ,"FROM syscat.references "
                       ,"WHERE tabschema=current_schema "
                       ,"AND tabname=? "
                       ,"AND constname=? "
                       ]
        let ref = refName tname cname
        stmt <- getter $ pack sql
        runResourceT $ stmtQuery stmt
                     [ PersistText $ unDBName tname
                     , tracex ("LET REF=ref["++show ref++"] tname[" ++ show tname ++ "] cname[" ++ show cname ++ "]") $ PersistText $ unDBName ref
                     ] $$ do
            hd <- CL.head
            return $ case hd of
              Just [PersistByteString bs] -> tracex ("getRef PersistByteString tname[" ++ show tname ++ "] cname[" ++ show cname ++ "] bs=" ++ show (DBName $ TE.decodeUtf8 bs)) $ Just (DBName "", ref) -- (DBName $ TE.decodeUtf8 bs, ref)
              Nothing -> tracex ("getRef Nothing!!!! tname[" ++ show tname ++ "] cname[" ++ show cname ++ "]") Nothing
              xs -> error $ "unknown value returned " ++ show xs ++ " other="++show (tname,cname)
    d' = case d of
            PersistNull   -> Right Nothing
            PersistText t -> Right $ Just t
            PersistByteString bs -> Right $ Just $ TE.decodeUtf8 bs
            _ -> Left $ pack $ "Invalid default column: " ++ show d
    getType "SMALLINT"    _ = Right $ SqlInt32
    getType "BIGINT"      _ = Right $ SqlInt64
    getType "VARCHAR"     _ = Right $ SqlString
    getType "DATE"        _ = Right $ SqlDay
    getType "CHARACTER"   _ = Right $ SqlBool
    getType "TIMESTAMP"   _ = Right $ SqlDayTime
    getType "TIMESTAMP WITH TIMEZONE" _ = Right $ SqlDayTimeZoned
    getType "FLOAT"       _ = Right $ SqlReal
    getType "DOUBLE"      _ = Right $ SqlReal
    getType "DECIMAL"     _ = Right $ SqlReal
    getType "BLOB"        _ = Right $ SqlBlob
    getType "TIME"        _ = Right $ SqlTime
    getType "NUMERIC"     _ = getNumeric npre nscl
    getType a             _ = error $ "what is this type a="++ show a -- Right $ SqlOther a

    getNumeric (PersistInt64 a) (PersistInt64 b) = Right $ SqlNumeric (fromIntegral a) (fromIntegral b)
    getNumeric a b = Left $ pack $ "Can not get numeric field precision, got: " ++ show a ++ " and " ++ show b ++ " as precision and scale"
getColumn _ a2 x =
    return $ Left $ pack $ "Invalid result from information_schema: " ++ show x ++ " a2[" ++ show a2 ++ "]"

findAlters :: Column -> [Column] -> ([AlterColumn'], [Column])
findAlters col@(Column name isNull sqltype def defConstraintName _maxLen ref) cols = tracex ("findAlters col="++show col ++ " cols="++show cols) $ 
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
                modType = tracex ("modType: sqltype[" ++ show sqltype ++ "] sqltype'[" ++ show sqltype' ++ "] name=" ++ show name) $ 
                          if tpcheck sqltype sqltype' then [] else [(name, Type sqltype)]
                modDef = tracex ("modDef col=" ++ show col ++ " def=" ++ show def ++ " def'=" ++ show def') $
                    if cmpdef def def'
                        then []
                        else case def of
                                Nothing -> [(name, NoDefault)]
                                Just s -> [(name, Default $ T.unpack s)]
             in (modRef ++ modDef ++ modNull ++ modType,
                 filter (\c -> cName c /= name) cols)

tpcheck :: SqlType -> SqlType -> Bool
tpcheck (SqlNumeric _ _) SqlReal = True -- else will try to migrate rational columns
tpcheck SqlReal (SqlNumeric _ _) = True
tpcheck a b = a==b

cmpdef::Maybe Text -> Maybe Text -> Bool
cmpdef Nothing Nothing = True
cmpdef (Just def) (Just def') | def==def' = True
                              | otherwise = 
        let (a,_)=T.breakOnEnd ":" def'
        in -- tracex ("cmpdef def[" ++ show def ++ "] def'[" ++ show def' ++ "] a["++show a++"]") $ 
           case T.stripSuffix "::" a of
              Just xs -> def==xs
              Nothing -> False
cmpdef _ _ = False

-- | Get the references to be added to a table for the given column.
getAddReference :: DBName -> Column -> Maybe AlterDB
getAddReference table (Column n _nu _ _def _defConstraintName _maxLen ref) =
    case ref of
        Nothing -> Nothing
        Just (s, _) -> Just $ AlterColumn table (n, AddReference s)

showColumn :: Column -> String
showColumn (Column n nu sqlType def defConstraintName _maxLen _ref) = concat
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
showSqlType SqlString Nothing = "VARCHAR(1000)"
showSqlType SqlString (Just len) = "VARCHAR(" ++ show len ++ ")"
showSqlType SqlInt32 _ = "SMALLINT"
showSqlType SqlInt64 _ = "BIGINT"
showSqlType SqlReal _ = "DOUBLE PRECISION"
showSqlType (SqlNumeric s prec) _ = "NUMERIC(" ++ show prec ++ ")"
showSqlType SqlDay _ = "DATE"
showSqlType SqlTime _ = "TIME"
showSqlType SqlDayTime _ = "TIMESTAMP"
showSqlType SqlDayTimeZoned _ = "TIMESTAMP WITH TIME ZONE"
showSqlType SqlBlob _ = "BLOB"
showSqlType SqlBool Nothing = "CHARACTER"
showSqlType SqlBool (Just 1) = "CHARACTER"
showSqlType SqlBool (Just n) = "CHARACTER(" ++ show n ++ ")"
showSqlType (SqlOther t) _ = T.unpack t
showSqlType (SqlManyKeys t) _ = error $ "this should not be called:internally used for composite primary keys " ++ show t

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
        , " SET DATA TYPE "
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

pkeyName :: DBName -> DBName
pkeyName (DBName table) =
    DBName $ T.concat [table, "_pkey"]

udToPair :: UniqueDef -> (DBName, [DBName])
udToPair ud = (uniqueDBName ud, map snd $ uniqueFields ud)

insertSql' :: DBName -> [FieldDef SqlType] -> DBName -> [PersistValue] -> Bool -> InsertSqlResult
insertSql' t cols _ vals True =
  let keypair = case vals of
                  (PersistInt64 _:PersistInt64 _:_) -> map (\(PersistInt64 i) -> i) vals -- gb fix unsafe
                  _ -> error $ "unexpected vals returned: vals=" ++ show vals
  in tracex ("yes ISRManyKeys!!! sql="++show sql) $
      ISRManyKeys sql keypair 
        where sql = pack $ concat
                [ "INSERT INTO "
                , T.unpack $ escape t
                , "("
                , intercalate "," $ map (T.unpack . escape . fieldDB) $ filter (\fd -> null $ fieldManyDB fd) cols
                , ") VALUES("
                , intercalate "," (map (const "?") cols)
                , ")"
                ]

insertSql' t cols _ vals False = 
  tracex "isrinsertget" $
    ISRInsertGet doInsert "select IDENTITY_VAL_LOCAL() as last_cod from sysibm.sysdummy1" 
    where
      doInsert = pack $ concat
        [ "INSERT INTO "
        , T.unpack $ escape t
        , "("
        , intercalate "," $ map (T.unpack . escape . fieldDB) cols
        , ") VALUES("
        , intercalate "," $ zipWith doValue cols vals
        , ")"
        ]
      doValue f@FieldDef { fieldSqlType = SqlBlob } PersistNull = error $ "persistent-odbc db2 currently doesn't support inserting nulls in a blob field f=" ++ show f -- tracex "\n\nin blob with null\n\n" "iif(? is null, convert(varbinary(max), cast ('' as nvarchar(max))), convert(varbinary(max), cast ('' as nvarchar(max))))"
      doValue FieldDef { fieldSqlType = SqlBlob } (PersistByteString _) = "blob(?)" -- tracex "\n\nin blob with a value\n\n" "blob(?)"
      doValue _ _ = "?"
