{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE CPP #-}
-- | A MSSQL backend for @persistent@.
module Database.Persist.MigrateMSSQL
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
import Data.Monoid ((<>), mconcat)

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
getMigrationStrategy dbtype@MSSQL { mssql2012=ok } = 
     MigrationStrategy
                          { dbmsLimitOffset=limitOffset ok
                           ,dbmsMigrate=migrate'
                           ,dbmsInsertSql=insertSql'
                           ,dbmsEscape=T.pack . escapeDBName
                           ,dbmsType=dbtype
                          }
getMigrationStrategy dbtype = error $ "MSSQL: calling with invalid dbtype " ++ show dbtype

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
                Nothing  -> concat [escapeDBName $ fieldDB $ entityId val, " BIGINT NOT NULL IDENTITY(1,1) PRIMARY KEY "]
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
                 | NoDefault DBName
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
    stmtIdClmn <- getter $ mconcat $ 
      ["SELECT COLUMN_NAME, "
      ,"IS_NULLABLE, "
      ,"DATA_TYPE, "
      ,"COLUMN_DEFAULT "
      ,"FROM INFORMATION_SCHEMA.COLUMNS "
      ,"WHERE TABLE_NAME   = ? "
      ,"AND COLUMN_NAME  = ?"]
    inter1 <- with (stmtQuery stmtIdClmn vals) (`connect` CL.consume)
    ids <- runResourceT $ CL.sourceList inter1 `connect` helperClmns -- avoid nested queries

    -- Find out all columns.
    let sql=mconcat [
                    "select info.COLUMN_NAME"
                    ," ,info.IS_NULLABLE,info.DATA_TYPE"
                    ," ,info.COLUMN_DEFAULT"
                    ," ,OBJECT_NAME(con.constid) AS DEFAULT_CONSTRAINT_NAME "
                    ," FROM sys.columns col "
                    ," inner join sys.tables tab on col.object_id=tab.object_id "
                    ," join INFORMATION_SCHEMA.COLUMNS info on info.table_name=? "
                    ," and tab.name=info.table_name "
                    ," and col.name=info.COLUMN_NAME  "
                    ," AND COLUMN_NAME  <> ?"
                    ," LEFT OUTER JOIN sysconstraints con "
                    ," ON con.constid=col.default_object_id "
                    ]     
    --liftIO $ putStrLn $ "sql=" ++ show sql                
    stmtClmns <- getter sql                     
    inter2 <- with (stmtQuery stmtClmns vals) (`connect` CL.consume)
    cs <- runResourceT $ CL.sourceList inter2 `connect` helperClmns -- avoid nested queries

    -- Find out the constraints.
    stmtCntrs <- getter $ mconcat $ 
      ["SELECT CONSTRAINT_NAME, "
      ,"COLUMN_NAME "
      ,"FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE p "
      ,"WHERE TABLE_NAME   = ? "
      ,"AND COLUMN_NAME <> ? "
      ,"AND exists (select 1 from INFORMATION_SCHEMA.TABLE_CONSTRAINTs AS TC where tc.CONSTRAINT_NAME=p.CONSTRAINT_NAME and tc.constraint_type<>'PRIMARY KEY') "
      ,"AND not exists (select 1 from INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS RC where rc.CONSTRAINT_NAME=p.CONSTRAINT_NAME) "
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
                                   , default'
                                   , defaultConstraintName'] =
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

      -- Default Constraint name
      defaultConstraintName_ <- case defaultConstraintName' of
                    PersistNull   -> return Nothing
                    PersistText t -> return $ Just $ DBName t
                    PersistByteString bs ->
                      case T.decodeUtf8' bs of
                        Left exc -> fail $ "Invalid default constraintname: " ++
                                           show defaultConstraintName' ++ " (error: " ++
                                           show exc ++ ")"
                        Right t  -> return $ Just $ DBName t
                    _ -> fail $ "Invalid default constraint name: " ++ show defaultConstraintName'
      -- Column type
      type_ <- parseType type'

      -- Foreign key (if any)
      stmt <- lift $ getter $ mconcat $
         ["SELECT KCU2.TABLE_NAME AS REFERENCED_TABLE_NAME, "
        ,"KCU1.CONSTRAINT_NAME AS CONSTRAINT_NAME, 1 "
        ,"FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS RC "
        ,"INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KCU1 " 
        ,"ON KCU1.CONSTRAINT_CATALOG = RC.CONSTRAINT_CATALOG " 
        ,"AND KCU1.CONSTRAINT_SCHEMA = RC.CONSTRAINT_SCHEMA " 
        ,"AND KCU1.CONSTRAINT_NAME = RC.CONSTRAINT_NAME " 
        ,"INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KCU2 " 
        ,"ON KCU2.CONSTRAINT_CATALOG = RC.UNIQUE_CONSTRAINT_CATALOG " 
        ,"AND KCU2.CONSTRAINT_SCHEMA = RC.UNIQUE_CONSTRAINT_SCHEMA "
        ,"AND KCU2.CONSTRAINT_NAME = RC.UNIQUE_CONSTRAINT_NAME "
        ,"AND KCU2.ORDINAL_POSITION = KCU1.ORDINAL_POSITION "
        ,"where KCU1.TABLE_NAME = ? and KCU1.COLUMN_NAME = ? "
        ,"order by CONSTRAINT_NAME, KCU1.COLUMN_NAME"]

      let vars = [ PersistText $ unDBName tname
                 , PersistText $ T.decodeUtf8 cname
                 ]
      cntrs <- liftIO $ with (stmtQuery stmt vars) (`connect` CL.consume)
      ref <- case cntrs of
               [] -> return Nothing
               [[PersistByteString tab, PersistByteString ref, PersistInt64 pos]] ->
                   tracex ("\n\n\nGBREF "++show (tab,ref,pos)++"\n\n") $ 
                    return $ Just (DBName $ T.decodeUtf8 tab, DBName $ T.decodeUtf8 ref)
               a1 -> fail $ "MSSQL.getColumn/getRef: never here error[" ++ show a1 ++ "]"

      -- Okay!
      return Column
        { cName = DBName $ T.decodeUtf8 cname
        , cNull = null_ == "YES"
        , cSqlType = type_
        , cDefault = default_
        , cDefaultConstraintName = defaultConstraintName_
        , cMaxLen = Nothing -- FIXME: maxLen
        , cReference = ref
        }

getColumn _ _ x =
    return $ Left $ pack $ "Invalid result from INFORMATION_SCHEMA: " ++ show x


-- | Parse the type of column as returned by MSSQL's
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
parseType "real"       = return SqlReal
parseType "float"      = return SqlReal
parseType "double"     = return SqlReal
parseType "decimal"    = return SqlReal
parseType "numeric"    = return SqlReal
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
parseType "image"   = return SqlBlob
-- Time-related
parseType "time"       = return SqlTime
parseType "datetime"   = return SqlDayTime
parseType "datetime2"  = return SqlDayTime
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
                modDef | def == def' = []
                       | otherwise   = tracex ("\n\nfindAlters modDef col=" ++ show col ++ " name=" ++ show name ++ " def=" ++ show def ++ " def'=" ++ show def' ++ " defConstraintName=" ++ show defConstraintName++" defConstraintName'=" ++ show defConstraintName' ++ "\n\n") $ 
                                        case def of
                                         Nothing -> [(name, NoDefault $ maybe (error $ "expected a constraint name col="++show name) id defConstraintName')]
                                         Just s -> if cmpdef def def' then [] else [(name, Default $ T.unpack s)]
            in ( refDrop ++ modType ++ modDef ++ refAdd
               , filter ((name /=) . cName) cols )

cmpdef::Maybe Text -> Maybe Text -> Bool
cmpdef Nothing Nothing = True
cmpdef (Just def) (Just def') = "(" <> def <> ")" == def'
cmpdef _ _ = False

tpcheck :: SqlType -> SqlType -> Bool
tpcheck (SqlNumeric _ _) SqlReal = True
tpcheck SqlReal (SqlNumeric _ _) = True
tpcheck a b = a==b

----------------------------------------------------------------------


-- | Prints the part of a @CREATE TABLE@ statement about a given
-- column.
showColumn :: Column -> String
showColumn (Column n nu t def _defConstraintName maxLen _ref) = concat
    [ escapeDBName n
    , " "
    , showSqlType t maxLen
    , " "
    , if nu then "NULL" else "NOT NULL"
    , case def of
        Nothing -> ""
        Just s -> " DEFAULT " ++ T.unpack s
    ]


-- | Renders an 'SqlType' in MSSQL's format.
showSqlType :: SqlType
            -> Maybe Integer -- ^ @maxlen@
            -> String
showSqlType SqlBlob    Nothing    = "VARBINARY(MAX)"
showSqlType SqlBlob    (Just i)   = "VARBINARY(" ++ show i ++ ")"
showSqlType SqlBool    _          = "TINYINT"
showSqlType SqlDay     _          = "DATE"
showSqlType SqlDayTime _          = "DATETIME2"
--showSqlType SqlDayTimeZoned _     = "VARCHAR(50)"
showSqlType SqlInt32   _          = "INT"
showSqlType SqlInt64   _          = "BIGINT"
showSqlType SqlReal    _          = "REAL"
showSqlType (SqlNumeric s prec) _ = "NUMERIC(" ++ show s ++ "," ++ show prec ++ ")"
showSqlType SqlString  Nothing    = "VARCHAR(1000)"
showSqlType SqlString  (Just i)   = "VARCHAR(" ++ show i ++ ")"
showSqlType SqlTime    _          = "TIME"
showSqlType (SqlOther t) _        = error ("showSqlType unhandled type t="++show t)   -- T.unpack t

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
    , intercalate "," $ map (escapeDBName . fst) cols
    , ")"
    ]
showAlterTable table (DropUniqueConstraint cname) = concat
    [ "ALTER TABLE "
    , escapeDBName table
    , " DROP "
    , escapeDBName cname
    ]


-- | Render an action that must be done on a column.
showAlter :: DBName -> AlterColumn' -> String
showAlter table (_oldName, Change (Column n nu t def defConstraintName maxLen _ref)) =
    concat
    [ "ALTER TABLE "
    , escapeDBName table
    , " ALTER COLUMN "
    , showColumn (Column n nu t def defConstraintName maxLen Nothing)
    ]
showAlter table (_, Add' col) =
    concat
    [ "ALTER TABLE "
    , escapeDBName table
    , " ADD "
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
    , " ADD DEFAULT "
    , s
    , " FOR "
    , escapeDBName n
    ]
showAlter table (_n, NoDefault defConstraintName) =
    concat
    [ "ALTER TABLE "
    , escapeDBName table
    , " DROP CONSTRAINT "
    , escapeDBName defConstraintName
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
    , " DROP CONSTRAINT "
    , escapeDBName cname
    ]

refName :: DBName -> DBName -> DBName
refName (DBName table) (DBName column) =
    DBName $ T.concat [table, "_", column, "_fkey"]

----------------------------------------------------------------------


-- | Escape a database name to be included on a query.
escapeDBName :: DBName -> String
escapeDBName (DBName s) = '[' : go (T.unpack s)
    where
      go (']':xs) = ']' : ']' : go xs
      go ( x :xs) =     x     : go xs
      go ""       = "]"
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
    Nothing -> 
-- should use scope_identity() but doesnt work :gives null
      ISRInsertGet doInsert "SELECT @@identity"
      where
        doInsert = pack $ concat
          [ "INSERT INTO "
          , escapeDBName $ entityDB ent
          , "("
          , intercalate "," $ map (escapeDBName . fieldDB) $ entityFields ent
          , ") VALUES("
          , intercalate "," $ zipWith doValue (entityFields ent) vals
          , ")"
          ]
        --doValue (FieldDef { fieldSqlType = SqlBlob }, PersistByteString _) = tracex "\n\nin blob with a value\n\n" "convert(varbinary(max),convert(varbinary(max),?))"
        --doValue (FieldDef { fieldSqlType = SqlBlob }, PersistByteString _) = tracex "\n\nin blob with a value\n\n" "convert(varbinary(max), cast (? as varchar(1000)))"
        doValue f@FieldDef { fieldSqlType = SqlBlob } PersistNull = error $ "persistent-odbc mssql currently doesn't support inserting nulls in a blob field f=" ++ show f -- tracex "\n\nin blob with null\n\n" "iif(? is null, convert(varbinary(max), cast ('' as nvarchar(max))), convert(varbinary(max), cast ('' as nvarchar(max))))"
        doValue FieldDef { fieldSqlType = SqlBlob } (PersistByteString _) = "convert(varbinary(max),?)" -- tracex "\n\nin blob with a value\n\n" "convert(varbinary(max),?)"
  --      doValue (FieldDef { fieldSqlType = SqlBlob }, PersistNull) = tracex "\n\nin blob with null\n\n" "iif(? is null, convert(varbinary(max),''), convert(varbinary(max),''))"
  --      doValue (FieldDef { fieldSqlType = SqlBlob }, PersistNull) = tracex "\n\nin blob with null\n\n" "isnull(?,'')" -- or 0x not in quotes
  --      doValue (FieldDef { fieldSqlType = SqlBlob }, PersistNull) = tracex "\n\nin blob with null\n\n" "isnull(?,convert(varbinary(max),''))"
        doValue _ _ = "?"

        
limitOffset::Bool -> (Int,Int) -> Bool -> Text -> Text 
limitOffset mssql2012' (limit,offset) hasOrder sql 
   | limit==0 && offset==0 = sql
   | mssql2012' && hasOrder && limit==0 = sql <> " offset " <> T.pack (show offset) <> " rows"
   | mssql2012' && hasOrder = sql <> " offset " <> T.pack (show offset) <> " rows fetch next " <> T.pack (show limit) <> " rows only"
   | not mssql2012' && offset==0 = case "select " `T.isPrefixOf` T.toLower (T.take 7 sql) of
                                    True -> let (a,b) = T.splitAt 7 sql 
                                                ret=a <> T.pack "top " <> T.pack (show limit) <> " " <> b
                                            in ret -- tracex ("ret="++show ret) ret
                                    False -> if T.null sql then error "MSSQL: not 2012 so trying to add 'top n' but the sql is empty"
                                              else error $ "MSSQL: not 2012 so trying to add 'top n' but is not a select sql=" ++ T.unpack sql
   | mssql2012' = error $ "MS SQL Server 2012 requires an order by statement for limit and offset sql=" ++ T.unpack sql
   | otherwise = error $ "MSSQL does not support limit and offset until MS SQL Server 2012 sql=" ++ T.unpack sql ++ " mssql2012=" ++ show mssql2012' ++ " hasOrder=" ++ show hasOrder
{-
limitOffset True (limit,offset) False sql = error "MS SQL Server 2012 requires an order by statement for limit and offset" 
limitOffset False (limit,offset) _ sql = error "MSSQL does not support limit and offset until MS SQL Server 2012"
limitOffset True (limit,offset) True sql = undefined
-}
