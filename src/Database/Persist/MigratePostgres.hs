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
import qualified Data.Text as T
import Data.Text (pack,Text)

import Data.Either (partitionEithers)
import Control.Arrow
import Data.List (find, intercalate, sort, groupBy)
import Data.Function (on)
import Data.Conduit (connect, (.|))
import qualified Data.Conduit.List as CL
import Data.Maybe (mapMaybe)

import qualified Data.Text.Encoding as T

import Database.Persist.ODBCTypes
import Data.Acquire (with)

#if DEBUG
import Debug.Trace
tracex::String -> a -> a
tracex = trace
#else
tracex::String -> a -> a
tracex _ b = b
#endif

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
                     
migrate' :: [EntityDef]
         -> (Text -> IO Statement)
         -> EntityDef
         -> IO (Either [Text] [(Bool, Text)])
migrate' allDefs getter val = fmap (fmap $ map showAlterDb) $ do
    let name = entityDB val
    old <- getColumns getter val
    case partitionEithers old of
        ([], old'') -> do
            let old' = partitionEithers old''
            let (newcols', udefs, fdefs) = mkColumns allDefs val
            let newcols = filter (not . safeToRemove val . cName) newcols'
            let udspair = map udToPair udefs
            --let composite = isJust $ entityPrimary val
            if null old
                then do
                    let idtxt = case entityPrimary val of
                                  Just pdef -> concat [" PRIMARY KEY (", intercalate "," $ map (T.unpack . escape . fieldDB) $ compositeFields pdef, ")"]
                                  Nothing   -> concat [T.unpack $ escape $ fieldDB $ entityId val
                                        , " SERIAL PRIMARY KEY UNIQUE"]
                    let addTable = AddTable $ concat
                            -- Lower case e: see Database.Persist.Sql.Migration
                            [ "CREATe TABLE "
                            , T.unpack $ escape name
                            , "("
                            , idtxt
                            , if null newcols then [] else ","
                            , intercalate "," $ map showColumn newcols
                            , ")"
                            ]
                    let uniques = flip concatMap udspair $ \(uname, ucols) ->
                            [AlterTable name $ AddUniqueConstraint uname ucols]
                        references = mapMaybe (\c@Column { cName=cname, cReference=Just (refTblName, _) } -> getAddReference allDefs name refTblName cname (cReference c)) $ filter (\c -> cReference c /= Nothing) newcols
                        foreignsAlt = map (\fdef -> let (childfields, parentfields) = unzip (map (\((_,b),(_,d)) -> (b,d)) (foreignFields fdef)) 
                                                    in AlterColumn name (foreignRefTableDBName fdef, AddReference (foreignConstraintNameDBName fdef) childfields parentfields)) fdefs
                    return $ Right $ addTable : uniques ++ references ++ foreignsAlt
                else do
                    let (acs, ats) = getAlters allDefs val (newcols, udspair) old'
                    let acs' = map (AlterColumn name) acs
                    let ats' = map (AlterTable name) ats
                    return $ Right $ acs' ++ ats'
        (errs, _) -> return $ Left errs

type SafeToRemove = Bool

data AlterColumn = Type SqlType | IsNull | NotNull | Add' Column | Drop SafeToRemove
                 | Default String | NoDefault | Update' String
                 | AddReference DBName [DBName] [DBName] | DropReference DBName
type AlterColumn' = (DBName, AlterColumn)

data AlterTable = AddUniqueConstraint DBName [DBName]
                | DropConstraint DBName

data AlterDB = AddTable String
             | AlterColumn DBName AlterColumn'
             | AlterTable DBName AlterTable

-- | Returns all of the columns in the given table currently in the database.
getColumns :: (Text -> IO Statement)
           -> EntityDef
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
                          ,"AND table_schema=current_schema() "
                          ,"AND table_name=? "
                          ,"AND column_name <> ?"]
  
    stmt <- getter $ pack sqlv
    let vals =
            [ PersistText $ unDBName $ entityDB def
            , PersistText $ unDBName $ fieldDB $ entityId def
            ]
    cs <- with (stmtQuery stmt vals) (`connect` helperClmns)
    let sqlc=concat ["SELECT "
                          ,"c.constraint_name, "
                          ,"c.column_name "
                          ,"FROM information_schema.key_column_usage c, "
                          ,"information_schema.table_constraints k "
                          ,"WHERE c.table_catalog=current_database() "
                          ,"AND c.table_catalog=k.table_catalog "
                          ,"AND c.table_schema=current_schema() "
                          ,"AND c.table_schema=k.table_schema "
                          ,"AND c.table_name=? "
                          ,"AND c.table_name=k.table_name "
                          ,"AND c.column_name <> ? "
                          ,"AND c.ordinal_position=1 "
                          ,"AND c.constraint_name=k.constraint_name "
                          ,"AND k.constraint_type not in ('PRIMARY KEY','FOREIGN KEY') "
                          ,"ORDER BY c.constraint_name, c.column_name"]

    stmt' <- getter $ pack sqlc
        
    us <- with (stmtQuery stmt' vals) (`connect` helperU)
    liftIO $ putStrLn $ "\n\ngetColumns cs="++show cs++"\n\nus="++show us
    return $ cs ++ us
  where
    getAll front = do
        x <- CL.head
        case x of
            Nothing -> return $ front []
            Just [PersistText con, PersistText col] -> getAll (front . (:) (con, col))
            Just [PersistByteString con, PersistByteString col] -> getAll (front . (:) (T.decodeUtf8 con, T.decodeUtf8 col)) 
            Just xx -> error $ "oops: unexpected datatype returned odbc postgres  xx="++show xx
    helperU = do
        rows <- getAll id
        return $ map (Right . Right . (DBName . fst . head &&& map (DBName . snd)))
               $ groupBy ((==) `on` fst) rows

    helperClmns = CL.mapM getIt .| CL.consume
        where
          getIt = fmap (either Left (Right . Left)) .
                  liftIO .
                  getColumn getter (entityDB def)
{-
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
-}
-- | Check if a column name is listed as the "safe to remove" in the entity
-- list.
safeToRemove :: EntityDef -> DBName -> Bool
safeToRemove def (DBName colName)
    = any (elem "SafeToRemove" . fieldAttrs)
    $ filter ((== (DBName colName)) . fieldDB)
    $ entityFields def

getAlters :: [EntityDef]
          -> EntityDef
          -> ([Column], [(DBName, [DBName])])
          -> ([Column], [(DBName, [DBName])])
          -> ([AlterColumn'], [AlterTable])
getAlters allDefs def (c1, u1) (c2, u2) =
    (getAltersC c1 c2, getAltersU u1 u2)
  where
    getAltersC [] old = map (\x -> (cName x, Drop $ safeToRemove def $ cName x)) old
    getAltersC (new:news) old =
        let (alters, old') = findAlters allDefs (entityDB def) new old
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
            case getType (T.decodeUtf8 z) of
                Left s -> return $ Left s
                Right t -> do
                    let cname = DBName $ T.decodeUtf8 x
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
        let sql = pack $ concat
                [ "SELECT "
                  ,"tc.table_name, "
                  ,"kcu.column_name, "
                  ,"ccu.table_name AS foreign_table_name, "
                  ,"ccu.column_name AS foreign_column_name, "
                  ,"kcu.ordinal_position "
                  ,"FROM "
                  ,"information_schema.table_constraints AS tc "
                  ,"JOIN information_schema.key_column_usage "
                  ,"AS kcu ON tc.constraint_name = kcu.constraint_name "
                  ,"JOIN information_schema.constraint_column_usage "
                  ,"AS ccu ON ccu.constraint_name = tc.constraint_name "
                  ,"WHERE constraint_type = 'FOREIGN KEY' "
                  ,"and tc.table_name=? "
                  ,"and tc.constraint_name=? "
                  ,"and tc.table_catalog=current_database() "
                  ,"AND tc.table_catalog=kcu.table_catalog "
                  ,"AND tc.table_catalog=ccu.table_catalog "
                  ,"AND tc.table_schema=current_schema() "
                  ,"AND tc.table_schema=kcu.table_schema "
                  ,"AND tc.table_schema=ccu.table_schema "
                  ,"order by tc.table_name,tc.constraint_name, kcu.ordinal_position " 
                  ]

        let ref = refName tname cname
        stmt <- getter sql
        with (stmtQuery stmt
                     [ PersistText $ unDBName tname
                     , PersistText $ unDBName ref
                     ]) (`connect` do
            m <- CL.head

            return $ case m of
              Just [PersistText _table, PersistText _col, PersistText reftable, PersistText _refcol, PersistInt64 _pos] -> Just (DBName reftable, ref)
              Just [PersistByteString _table, PersistByteString _col, PersistByteString reftable, PersistByteString _refcol, PersistInt64 _pos] -> Just (DBName (T.decodeUtf8 reftable), ref)
              Nothing -> Nothing
              _ -> error $ "unexpected result found ["++ show m ++ "]" )
    d' = case d of
            PersistNull   -> Right Nothing
            PersistText t -> Right $ Just t
            PersistByteString bs -> Right $ Just $ T.decodeUtf8 bs
            _ -> Left $ pack $ "Invalid default column: " ++ show d
    getType "int4"        = Right $ SqlInt32
    getType "int8"        = Right $ SqlInt64
    getType "varchar"     = Right $ SqlString
    getType "date"        = Right $ SqlDay
    getType "bool"        = Right $ SqlBool
    getType "timestamp"   = Right $ SqlDayTime
--    getType "timestamptz" = Right $ SqlDayTimeZoned
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

findAlters :: [EntityDef] -> DBName -> Column -> [Column] -> ([AlterColumn'], [Column])
findAlters defs tablename col@(Column name isNull sqltype def _defConstraintName _maxLen ref) cols =
    tracex ("\n\n\nfindAlters tablename="++show tablename++ " name="++ show name++" col="++show col++"\ncols="++show cols++"\n\n\n") $ case filter ((name ==) . cName) cols of
        [] -> ([(name, Add' col)], cols)
        Column _ isNull' sqltype' def' defConstraintName' _maxLen' ref':_ ->
            let refDrop Nothing = []
                refDrop (Just (_, cname)) = tracex ("\n\n\n44444 findAlters dropping fkey defConstraintName'="++show defConstraintName' ++" name="++show name++" cname="++show cname++" tablename="++show tablename++"\n\n\n") $ 
                                             [(name, DropReference cname)]
                refAdd Nothing = []
                refAdd (Just (tname, a)) = tracex ("\n\n\n33333 findAlters adding fkey defConstraintName'="++show defConstraintName' ++" name="++show name++" tname="++show tname++" a="++show a++" tablename="++show tablename++"\n\n\n") $ 
                                           case find ((==tname) . entityDB) defs of
                                                Just refdef -> [(tname, AddReference a [name] [fieldDB $ entityId refdef])]
                                                Nothing -> error $ "could not find the entityDef for reftable[" ++ show tname ++ "]"
                modRef = tracex ("modType: sqltype[" ++ show sqltype ++ "] sqltype'[" ++ show sqltype' ++ "] name=" ++ show name) $ 
                    if fmap snd ref == fmap snd ref'
                        then []
                        else tracex ("\n\n\nmodRef findAlters drop/add cos ref doesnt match ref[" ++ show ref ++ "] ref'[" ++ show ref' ++ "] tablename="++show tablename++"\n\n\n") $ 
                              refDrop ref' ++ refAdd ref
                modNull = case (isNull, isNull') of
                            (True, False) -> [(name, IsNull)]
                            (False, True) ->
                                let up = case def of
                                            Nothing -> id
                                            Just s -> (:) (name, Update' $ T.unpack s)
                                 in up [(name, NotNull)]
                            _ -> []
                modType = tracex ("modType: sqltype[" ++ show sqltype ++ "] sqltype'[" ++ show sqltype' ++ "] name=" ++ show name) $ 
                          if sqltype == sqltype' then [] else [(name, Type sqltype)]
                modDef = tracex ("modDef col=" ++ show col ++ " def=" ++ show def ++ " def'=" ++ show def') $
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
        let (a,_)=T.breakOnEnd ":" def'
        in -- tracex ("cmpdef def[" ++ show def ++ "] def'[" ++ show def' ++ "] a["++show a++"]") $ 
           case T.stripSuffix "::" a of
              Just xs -> def==xs
              Nothing -> False
cmpdef _ _ = False

-- | Get the references to be added to a table for the given column.
getAddReference :: [EntityDef] -> DBName -> DBName -> DBName -> Maybe (DBName, DBName) -> Maybe AlterDB
getAddReference allDefs table reftable cname ref =
    case ref of
        Nothing -> Nothing
        Just (s, z) -> tracex ("\n\ngetaddreference table="++ show table++" reftable="++show reftable++" s="++show s++" z=" ++ show z++"\n\n") $ 
                       Just $ AlterColumn table (s, AddReference (refName table cname) [cname] [id_])
                          where
                            id_ = maybe (error $ "Could not find ID of entity " ++ show reftable)
                                        id $ do
                                          entDef <- find ((== reftable) . entityDB) allDefs
                                          return (fieldDB $ entityId entDef)
                          

showColumn :: Column -> String
showColumn (Column n nu sqlType' def _defConstraintName _maxLen _ref) = concat
    [ T.unpack $ escape n
    , " "
    , showSqlType sqlType' _maxLen
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
--showSqlType SqlDayTimeZoned _ = "TIMESTAMP WITH TIME ZONE"
showSqlType SqlBlob _ = "BYTEA"
showSqlType SqlBool _ = "BOOLEAN"
showSqlType (SqlOther t) _ = T.unpack t

showAlterDb :: AlterDB -> (Bool, Text)
showAlterDb (AddTable s) = (False, pack s)
showAlterDb (AlterColumn t (c, ac)) =
    (isUnsafe ac, pack $ showAlter t (c, ac))
  where
    isUnsafe (Drop safeToRem) = not safeToRem
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
showAlter table (reftable, AddReference fkeyname t2 id2) = concat
    [ "ALTER TABLE "
    , T.unpack $ escape table
    , " ADD CONSTRAINT "
    , T.unpack $ escape fkeyname
    , " FOREIGN KEY("
    , T.unpack $ T.intercalate "," $ map escape t2
    , ") REFERENCES "
    , T.unpack $ escape reftable
    , "("
    , T.unpack $ T.intercalate "," $ map escape id2
    , ")"
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

insertSql' :: EntityDef -> [PersistValue] -> InsertSqlResult
insertSql' ent vals = tracex ("\n\n\nGBTEST " ++ show (entityFields ent) ++ "\n\n\n") $
  case entityPrimary ent of
    Just _pdef -> 
      ISRManyKeys sql vals
        where sql = pack $ concat
                [ "INSERT INTO "
                , T.unpack $ escape $ entityDB ent
                , "("
                , intercalate "," $ map (T.unpack . escape . fieldDB) $ entityFields ent
                , ") VALUES("
                , intercalate "," (map (const "?") $ entityFields ent)
                , ")"
                ]
    Nothing -> 
      ISRSingle $ pack $ concat
        [ "INSERT INTO "
        , T.unpack $ escape $ entityDB ent
        , "("
        , intercalate "," $ map (T.unpack . escape . fieldDB) $ entityFields ent
        , ") VALUES("
        , intercalate "," (map (const "?") $ entityFields ent)
        , ") RETURNING "
        , T.unpack $ escape $ fieldDB $ entityId ent
        ]
