{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE CPP #-}
-- | A MySQL backend for @persistent@.
module Database.Persist.MigrateMySQL
    ( getMigrationStrategy 
    ) where

import Control.Arrow
import Control.Monad.IO.Class (MonadIO (..))
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Except (runExceptT)
import Control.Monad.Trans.Resource (runResourceT)
import Data.ByteString (ByteString)
import Data.Either (partitionEithers)
import Data.Function (on)
import Data.List (find, intercalate, sort, groupBy)
import Data.Text (Text, pack)
import Data.Monoid ((<>))

import Data.Conduit (connect, (.|))
import qualified Data.Conduit.List as CL
import qualified Data.Text as T
import qualified Data.Text.Encoding as T

import Database.Persist.Sql
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
getMigrationStrategy dbtype@MySQL {} = 
     MigrationStrategy
                          { dbmsLimitOffset=decorateSQLWithLimitOffset "LIMIT 18446744073709551615" 
                           ,dbmsMigrate=migrate'
                           ,dbmsInsertSql=insertSql'
                           ,dbmsEscape=T.pack . escapeDBName 
                           ,dbmsType=dbtype
                          }
getMigrationStrategy dbtype = error $ "MySQL: calling with invalid dbtype " ++ show dbtype

-- | Create the migration plan for the given 'PersistEntity'
-- @val@.
migrate' :: [EntityDef]
         -> (Text -> IO Statement)
         -> EntityDef
         -> IO (Either [Text] [(Bool, Text)])
migrate' allDefs getter val = do
    let name = entityDB val
    (idClmn, old) <- getColumns getter val
    let (newcols, udefs, fdefs) = mkColumns allDefs val
    liftIO $ putStrLn $ "\n\nold="++show old
    liftIO $ putStrLn $ "\n\nfdefs="++show fdefs
    
    let udspair = map udToPair udefs
    case (idClmn, old, partitionEithers old) of
      -- Nothing found, create everything
      ([], [], _) -> do
        let idtxt = case entityPrimary val of
                Just pdef -> concat [" PRIMARY KEY (", intercalate "," $ map (escapeDBName . fieldDB) $ compositeFields pdef, ")"]
                Nothing   -> concat [escapeDBName $ fieldDB $ entityId val, " int NOT NULL AUTO_INCREMENT PRIMARY KEY"]

        let addTable = AddTable $ concat
                            -- Lower case e: see Database.Persist.Sql.Migration
                [ "CREATe TABLE "
                , escapeDBName name
                , "("
                , idtxt
                , if null newcols then [] else ","
                , intercalate "," $ map showColumn newcols
                , ")"
                ]
        let uniques = flip concatMap udspair $ \(uname, ucols) ->
                      [ AlterTable name $
                        AddUniqueConstraint uname $
                        map (findTypeOfColumn allDefs name) ucols ]
        let foreigns = tracex ("in migrate' newcols=" ++ show newcols) $ do
              Column { cName=cname, cReference=Just (refTblName, a) } <- newcols
              tracex ("\n\n111foreigns cname="++show cname++" name="++show name++" refTblName="++show refTblName++" a="++show a) $ 
               return $ AlterColumn name (refTblName, addReference allDefs (refName name cname) refTblName cname)
                 
        let foreignsAlt = map (\fdef -> let (childfields, parentfields) = unzip (map (\((_,b),(_,d)) -> (b,d)) (foreignFields fdef)) 
                                        in AlterColumn name (foreignRefTableDBName fdef, AddReference (foreignRefTableDBName fdef) (foreignConstraintNameDBName fdef) childfields parentfields)) fdefs
        
        return $ Right $ map showAlterDb $ addTable : uniques ++ foreigns ++ foreignsAlt
      -- No errors and something found, migrate
      (_, _, ([], old')) -> do
        let excludeForeignKeys (xs,ys) = (map (\c -> case cReference c of
                                                    Just (_,fk) -> case find (\f -> fk == foreignConstraintNameDBName f) fdefs of
                                                                     Just _ -> tracex ("\n\n\nremoving cos a composite fk="++show fk) $ 
                                                                                c { cReference = Nothing }
                                                                     Nothing -> c
                                                    Nothing -> c) xs,ys)
            (acs, ats) = getAlters allDefs name (newcols, udspair) $ excludeForeignKeys $ partitionEithers old'
            acs' = map (AlterColumn name) acs
            ats' = map (AlterTable  name) ats
        return $ Right $ map showAlterDb $ acs' ++ ats'
      -- Errors
      (_, _, (errs, _)) -> return $ Left errs


-- | Find out the type of a column.
findTypeOfColumn :: [EntityDef] -> DBName -> DBName -> (DBName, FieldType)
findTypeOfColumn allDefs name col =
    maybe (error $ "Could not find type of column " ++
                   show col ++ " on table " ++ show name ++
                   " (allDefs = " ++ show allDefs ++ ")")
          ((,) col) $ do
            entDef   <- find ((== name) . entityDB) allDefs
            fieldDef <- find ((== col)  . fieldDB) (entityFields entDef)
            return (fieldType fieldDef)


-- | Helper for 'AddRefence' that finds out the 'entityId'.
addReference :: [EntityDef] -> DBName -> DBName -> DBName -> AlterColumn
addReference allDefs fkeyname reftable cname = tracex ("\n\naddreference cname="++show cname++" fkeyname="++show fkeyname++" reftable="++show reftable++" id_="++show id_) $ 
                                  AddReference reftable fkeyname [cname] [id_] 
    where
      id_ = maybe (error $ "Could not find ID of entity " ++ show reftable
                         ++ " (allDefs = " ++ show allDefs ++ ")")
                  id $ do
                    entDef <- find ((== reftable) . entityDB) allDefs
                    return (fieldDB $ entityId entDef)

data AlterColumn = Change Column
                 | Add' Column
                 | Drop
                 | Default String
                 | NoDefault
                 | Update' String
                 | AddReference DBName DBName [DBName] [DBName]
                 | DropReference DBName

type AlterColumn' = (DBName, AlterColumn)

data AlterTable = AddUniqueConstraint DBName [(DBName, FieldType)]
                | DropUniqueConstraint DBName

data AlterDB = AddTable String
             | AlterColumn DBName AlterColumn'
             | AlterTable DBName AlterTable


udToPair :: UniqueDef -> (DBName, [DBName])
udToPair ud = (uniqueDBName ud, map snd $ uniqueFields ud)

----------------------------------------------------------------------


-- | Returns all of the 'Column'@s@ in the given table currently
-- in the database.
getColumns :: (Text -> IO Statement)
           -> EntityDef
           -> IO ( [Either Text (Either Column (DBName, [DBName]))] -- ID column
                 , [Either Text (Either Column (DBName, [DBName]))] -- everything else
                 )
getColumns getter def = do
    -- Find out ID column.
    stmtIdClmn <- getter $ T.concat $ 
      ["SELECT COLUMN_NAME, "
      ,"IS_NULLABLE, "
      ,"DATA_TYPE, "
      ,"COLUMN_DEFAULT "
      ,"FROM INFORMATION_SCHEMA.COLUMNS "
      ,"WHERE  table_schema=schema() "
      ,"AND TABLE_NAME   = ? "
      ,"AND COLUMN_NAME  = ?"]
    inter1 <- with (stmtQuery stmtIdClmn vals) (`connect` CL.consume)
    ids <- runResourceT $ CL.sourceList inter1 `connect` helperClmns -- avoid nested queries

    -- Find out all columns.
    stmtClmns <- getter $ T.concat $ 
      ["SELECT COLUMN_NAME, "
      ,"IS_NULLABLE, "
      ,"DATA_TYPE, "
      ,"COLUMN_DEFAULT "
      ,"FROM INFORMATION_SCHEMA.COLUMNS "
      ,"WHERE  table_schema=schema() "
      ,"AND TABLE_NAME   = ? "
      ,"AND COLUMN_NAME <> ?"]
    inter2 <- with (stmtQuery stmtClmns vals) (`connect` CL.consume)
    cs <- runResourceT $ CL.sourceList inter2 `connect` helperClmns -- avoid nested queries

    -- Find out the constraints.
    stmtCntrs <- getter $ T.concat $ 
      ["SELECT CONSTRAINT_NAME, "
      ,"COLUMN_NAME "
      ,"FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE "
      ,"WHERE  table_schema=schema() "
      ,"AND TABLE_NAME   = ? "
      ,"AND COLUMN_NAME <> ? "
      ,"AND CONSTRAINT_NAME <> 'PRIMARY' "
      ,"AND REFERENCED_TABLE_SCHEMA IS NULL "
      ,"ORDER BY CONSTRAINT_NAME, "
      ,"COLUMN_NAME"]
    us <- with (stmtQuery stmtCntrs vals) (`connect` helperCntrs)
    liftIO $ putStrLn $ "\n\ngetColumns cs="++show cs++"\n\nus="++show us
    -- Return both
    return (ids, cs ++ us)
  where
    vals = [ PersistText $ unDBName $ entityDB def
           , PersistText $ unDBName $ fieldDB $ entityId def ]

    helperClmns = CL.mapM getIt .| CL.consume
        where
          getIt = fmap (either Left (Right . Left)) .
                  liftIO .
                  getColumn getter (entityDB def)

    helperCntrs = do
      let check [ PersistByteString cntrName
                , PersistByteString clmnName] = return ( T.decodeUtf8 cntrName
                                                       , T.decodeUtf8 clmnName )
          check other = fail $ "helperCntrs: unexpected " ++ show other
      rows <- mapM check =<< CL.consume
      return $ map (Right . Right . (DBName . fst . head &&& map (DBName . snd)))
             $ groupBy ((==) `on` fst) rows


-- | Get the information about a column in a table.
getColumn :: (Text -> IO Statement)
          -> DBName
          -> [PersistValue]
          -> IO (Either Text Column)
getColumn getter tname [ PersistByteString cname
                                   , PersistByteString null_
                                   , PersistByteString type'
                                   , default'] =
    fmap (either (Left . pack) Right) $
    runExceptT $ do
      -- Default value
      default_ <- case default' of
                    PersistNull   -> return Nothing
                    PersistText t -> return (Just t)
                    PersistByteString bs ->
                      case T.decodeUtf8' bs of
                        Left exc -> fail $ "Invalid default column: " ++
                                           show default' ++ " (error: " ++
                                           show exc ++ ")"
                        Right t  -> return (Just t)
                    _ -> fail $ "Invalid default column: " ++ show default'

      -- Column type
      type_ <- parseType type'

      -- Foreign key (if any)
      stmt <- lift $ getter $ T.concat $ 
        ["SELECT REFERENCED_TABLE_NAME, "
        ,"CONSTRAINT_NAME, "
        ,"ORDINAL_POSITION "
        ,"FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE "
        ,"WHERE  table_schema=schema() "
        ,"AND TABLE_NAME   = ? "
        ,"AND COLUMN_NAME  = ? "
        ,"and REFERENCED_TABLE_SCHEMA=schema() "
        ,"and ordinal_position = 1 "
        ,"ORDER BY CONSTRAINT_NAME, "
        ,"COLUMN_NAME"]
      let vars = [ PersistText $ unDBName tname
                 , PersistByteString cname
                 ]
      cntrs <- liftIO $ with (stmtQuery stmt vars) (`connect` CL.consume)
      ref <- case cntrs of
               [] -> return Nothing
               [[PersistByteString tab, PersistByteString ref, PersistInt64 pos]] ->
                   tracex ("\n\n\nGBREF "++show (tab,ref,pos)++"\n\n") $ 
                    return $ Just (DBName $ T.decodeUtf8 tab, DBName $ T.decodeUtf8 ref)
               a1 -> fail $ "MySQL.getColumn/getRef: never here error[" ++ show a1 ++ "]"

      -- Okay!
      return Column
        { cName = DBName $ T.decodeUtf8 cname
        , cNull = null_ == "YES"
        , cSqlType = type_
        , cDefault = default_
        , cDefaultConstraintName = Nothing
        , cMaxLen = Nothing -- FIXME: maxLen
        , cReference = ref
        }

getColumn _ _ x =
    return $ Left $ pack $ "Invalid result from INFORMATION_SCHEMA: " ++ show x


-- | Parse the type of column as returned by MySQL's
-- @INFORMATION_SCHEMA@ tables.
parseType :: Monad m => ByteString -> m SqlType
parseType "tinyint"    = return SqlBool
-- Ints
parseType "int"        = return SqlInt32
parseType "short"      = return SqlInt32
parseType "long"       = return SqlInt64
parseType "longlong"   = return SqlInt64
parseType "mediumint"  = return SqlInt32
parseType "bigint"     = return SqlInt64
-- Double
parseType "float"      = return SqlReal
parseType "double"     = return SqlReal
parseType "decimal"    = return SqlReal
parseType "newdecimal" = return SqlReal
-- Text
parseType "varchar"    = return SqlString
parseType "varstring"  = return SqlString
parseType "string"     = return SqlString
parseType "text"       = return SqlString
parseType "tinytext"   = return SqlString
parseType "mediumtext" = return SqlString
parseType "longtext"   = return SqlString
-- ByteString
parseType "varbinary"  = return SqlBlob
parseType "blob"       = return SqlBlob
parseType "tinyblob"   = return SqlBlob
parseType "mediumblob" = return SqlBlob
parseType "longblob"   = return SqlBlob
-- Time-related
parseType "time"       = return SqlTime
parseType "datetime"   = return SqlDayTime
parseType "timestamp"  = return SqlDayTime
parseType "date"       = return SqlDay
parseType "newdate"    = return SqlDay
parseType "year"       = return SqlDay
-- Other
parseType b            = error $ "no idea how to handle this type b=" ++ show b -- return $ SqlOther $ T.decodeUtf8 b


----------------------------------------------------------------------


-- | @getAlters allDefs tblName new old@ finds out what needs to
-- be changed from @old@ to become @new@.
getAlters :: [EntityDef]
          -> DBName
          -> ([Column], [(DBName, [DBName])])
          -> ([Column], [(DBName, [DBName])])
          -> ([AlterColumn'], [AlterTable])
getAlters allDefs tblName (c1, u1) (c2, u2) =
    (getAltersC c1 c2, getAltersU u1 u2)
  where
    getAltersC [] old = concatMap dropColumn old
    getAltersC (new:news) old =
        let (alters, old') = findAlters tblName allDefs new old
         in alters ++ getAltersC news old'

    dropColumn col =
      map ((,) (cName col)) $
        [DropReference n | Just (_, n) <- [cReference col]] ++
        [Drop]

    getAltersU [] old = map (DropUniqueConstraint . fst) old
    getAltersU ((name, cols):news) old =
        case lookup name old of
            Nothing ->
                AddUniqueConstraint name (map findType cols) : getAltersU news old
            Just ocols ->
                let old' = filter (\(x, _) -> x /= name) old
                 in if sort cols == ocols
                        then getAltersU news old'
                        else  DropUniqueConstraint name
                            : AddUniqueConstraint name (map findType cols)
                            : getAltersU news old'
        where
          findType = findTypeOfColumn allDefs tblName


-- | @findAlters newColumn oldColumns@ finds out what needs to be
-- changed in the columns @oldColumns@ for @newColumn@ to be
-- supported.
findAlters :: DBName -> [EntityDef] -> Column -> [Column] -> ([AlterColumn'], [Column])
findAlters tblName allDefs col@(Column name isNull type_ def defConstraintName _maxLen ref) cols =
    tracex ("\n\n\nfindAlters tablename="++show tblName++ " name="++ show name++" col="++show col++"\ncols="++show cols++"\n\n\n") $
      case filter ((name ==) . cName) cols of
        [] -> case ref of
               Nothing -> ([(name, Add' col)], [])
               Just (tname, b) -> let cnstr = tracex ("\n\ncols="++show cols++"\n\n2222findalters new foreignkey col["++showColumn col++"] name["++show name++"] tname["++show tname++"] b["++show b ++ "]") $ 
                                              [addReference allDefs (refName tblName name) tname name]
                                  in (map ((,) name) (Add' col : cnstr), cols)
        Column _ isNull' type_' def' defConstraintName' _maxLen' ref':_ ->
            let -- Foreign key
                refDrop = case (ref == ref', ref') of
                            (False, Just (_, cname)) -> tracex ("\n\n44444findalters dropping foreignkey cname[" ++ show cname ++ "] ref[" ++ show ref ++"]") $ 
                                                        [(name, DropReference cname)]
                            _ -> []
                refAdd  = case (ref == ref', ref) of
                            (False, Just (tname, cname)) -> tracex ("\n\n33333 findalters foreignkey has changed cname["++show cname++"] name["++show name++"] tname["++show tname++"] ref["++show ref++"] ref'["++show ref' ++ "]") $ 
                                                             [(tname, addReference allDefs (refName tblName name) tname name)]
                            _ -> []
                -- Type and nullability
                modType | tpcheck type_ type_' && isNull == isNull' = []
                        | otherwise = [(name, Change col)]
                -- Default value
                modDef | cmpdef def def' = []
                       | otherwise   = tracex ("\n\nfindAlters modDef col=" ++ show col ++ " name=" ++ show name ++ " def=" ++ show def ++ " def'=" ++ show def' ++ " defConstraintName=" ++ show defConstraintName++" defConstraintName'=" ++ show defConstraintName' ++ "\n\n") $ 
                                       case def of
                                         Nothing -> [(name, NoDefault)]
                                         Just s -> [(name, Default $ T.unpack s)]
            in ( refDrop ++ modType ++ modDef ++ refAdd
               , filter ((name /=) . cName) cols )


cmpdef::Maybe Text -> Maybe Text -> Bool
cmpdef Nothing Nothing = True
cmpdef (Just def) (Just def') = def == "'" <> def' <> "'"
cmpdef _ _ = False

tpcheck :: SqlType -> SqlType -> Bool
tpcheck SqlDayTime SqlString = True
tpcheck SqlString SqlDayTime = True
tpcheck (SqlNumeric _ _) SqlReal = True
tpcheck SqlReal (SqlNumeric _ _) = True
tpcheck a b = a==b
----------------------------------------------------------------------


-- | Prints the part of a @CREATE TABLE@ statement about a given
-- column.
showColumn :: Column -> String
showColumn (Column n nu t def _defConstraintName maxLen ref) = concat
    [ escapeDBName n
    , " "
    , showSqlType t maxLen
    , " "
    , if nu then "NULL" else "NOT NULL"
    , case def of
        Nothing -> ""
        Just s -> " DEFAULT " ++ T.unpack s
    , case ref of
        Nothing -> ""
        Just (s, _) -> " REFERENCES " ++ escapeDBName s
    ]


-- | Renders an 'SqlType' in MySQL's format.
showSqlType :: SqlType
            -> Maybe Integer -- ^ @maxlen@
            -> String
showSqlType SqlBlob    Nothing    = "BLOB"
showSqlType SqlBlob    (Just i)   = "VARBINARY(" ++ show i ++ ")"
showSqlType SqlBool    _          = "TINYINT(1)"
showSqlType SqlDay     _          = "DATE"
showSqlType SqlDayTime _          = "DATETIME" -- "VARCHAR(50) CHARACTER SET utf8" 
--showSqlType SqlDayTimeZoned _     = "VARCHAR(50) CHARACTER SET utf8"
showSqlType SqlInt32   _          = "INT"
showSqlType SqlInt64   _          = "BIGINT"
showSqlType SqlReal    _          = "DOUBLE PRECISION"
showSqlType (SqlNumeric s prec) _ = "NUMERIC(" ++ show s ++ "," ++ show prec ++ ")"
showSqlType SqlString  Nothing    = "TEXT CHARACTER SET utf8"
showSqlType SqlString  (Just i)   = "VARCHAR(" ++ show i ++ ") CHARACTER SET utf8"
showSqlType SqlTime    _          = "TIME"
showSqlType (SqlOther t) _        = T.unpack t

-- | Render an action that must be done on the database.
showAlterDb :: AlterDB -> (Bool, Text)
showAlterDb (AddTable s) = (False, pack s)
showAlterDb (AlterColumn t (c, ac)) =
    (isUnsafe ac, pack $ showAlter t (c, ac))
  where
    isUnsafe Drop = True
    isUnsafe _    = False
showAlterDb (AlterTable t at) = (False, pack $ showAlterTable t at)


-- | Render an action that must be done on a table.
showAlterTable :: DBName -> AlterTable -> String
showAlterTable table (AddUniqueConstraint cname cols) = concat
    [ "ALTER TABLE "
    , escapeDBName table
    , " ADD CONSTRAINT "
    , escapeDBName cname
    , " UNIQUE("
    , intercalate "," $ map escapeDBName' cols
    , ")"
    ]
    where
      escapeDBName' (name, FTTypeCon _ "Text"      ) = escapeDBName name ++ "(200)"
      escapeDBName' (name, FTTypeCon _ "String"    ) = escapeDBName name ++ "(200)"
      escapeDBName' (name, FTTypeCon _ "ByteString") = escapeDBName name ++ "(200)"
      escapeDBName' (name, _                       ) = escapeDBName name
showAlterTable table (DropUniqueConstraint cname) = concat
    [ "ALTER TABLE "
    , escapeDBName table
    , " DROP INDEX "
    , escapeDBName cname
    ]


-- | Render an action that must be done on a column.
showAlter :: DBName -> AlterColumn' -> String
showAlter table (oldName, Change (Column n nu t def defConstraintName maxLen _ref)) =
    concat
    [ "ALTER TABLE "
    , escapeDBName table
    , " CHANGE "
    , escapeDBName oldName
    , " "
    , showColumn (Column n nu t def defConstraintName maxLen Nothing)
    ]
showAlter table (_, Add' col) =
    concat
    [ "ALTER TABLE "
    , escapeDBName table
    , " ADD COLUMN "
    , showColumn col
    ]
showAlter table (n, Drop) =
    concat
    [ "ALTER TABLE "
    , escapeDBName table
    , " DROP COLUMN "
    , escapeDBName n
    ]
showAlter table (n, Default s) =
    concat
    [ "ALTER TABLE "
    , escapeDBName table
    , " ALTER COLUMN "
    , escapeDBName n
    , " SET DEFAULT "
    , s
    ]
showAlter table (n, NoDefault) =
    concat
    [ "ALTER TABLE "
    , escapeDBName table
    , " ALTER COLUMN "
    , escapeDBName n
    , " DROP DEFAULT"
    ]
showAlter table (n, Update' s) =
    concat
    [ "UPDATE "
    , escapeDBName table
    , " SET "
    , escapeDBName n
    , "="
    , s
    , " WHERE "
    , escapeDBName n
    , " IS NULL"
    ]
showAlter table (_, AddReference reftable fkeyname t2 id2) = concat
    [ "ALTER TABLE "
    , escapeDBName table
    , " ADD CONSTRAINT "
    , escapeDBName fkeyname
    , " FOREIGN KEY("
    , intercalate "," $ map escapeDBName t2
    , ") REFERENCES "
    , escapeDBName reftable
    , "("
    , intercalate "," $ map escapeDBName id2
    , ")"
    ]
showAlter table (_, DropReference cname) = concat
    [ "ALTER TABLE "
    , escapeDBName table
    , " DROP FOREIGN KEY "
    , escapeDBName cname
    ]

refName :: DBName -> DBName -> DBName
refName (DBName table) (DBName column) =
    DBName $ T.concat [table, "_", column, "_fkey"]

----------------------------------------------------------------------


-- | Escape a database name to be included on a query.
escapeDBName :: DBName -> String
escapeDBName (DBName s) = '`' : go (T.unpack s)
    where
      go ('`':xs) = '`' : '`' : go xs
      go ( x :xs) =     x     : go xs
      go ""       = "`"
-- | SQL code to be executed when inserting an entity.
insertSql' :: EntityDef -> [PersistValue] -> InsertSqlResult
insertSql' ent vals =
  case entityPrimary ent of
    Just _pdef -> 
      ISRManyKeys sql vals
        where sql = pack $ concat
                [ "INSERT INTO "
                , escapeDBName $ entityDB ent
                , "("
                , intercalate "," $ map (escapeDBName . fieldDB) $ entityFields ent
                , ") VALUES("
                , intercalate "," (map (const "?") $ entityFields ent)
                , ")"
                ]
    Nothing -> ISRInsertGet doInsert "SELECT LAST_INSERT_ID()"
     where
      doInsert = pack $ concat
        [ "INSERT INTO "
        , escapeDBName $ entityDB ent
        , "("
        , intercalate "," $ map (escapeDBName . fieldDB) $ entityFields ent
        , ") VALUES("
        , intercalate "," (map (const "?") $ entityFields ent)
        , ")"
        ]
