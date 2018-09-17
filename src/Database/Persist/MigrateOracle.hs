{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE CPP #-}
-- | A Oracle backend for @persistent@.
module Database.Persist.MigrateOracle
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

import Data.Conduit (connect, (.|))
import qualified Data.Conduit.List as CL
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import Data.Monoid ((<>))
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
getMigrationStrategy dbtype@Oracle { oracle12c=ok} = 
     MigrationStrategy
                          { dbmsLimitOffset=limitOffset ok
                           ,dbmsMigrate=migrate'
                           ,dbmsInsertSql=insertSql'
                           ,dbmsEscape=T.pack . escapeDBName 
                           ,dbmsType=dbtype
                          }
getMigrationStrategy dbtype = error $ "Oracle: calling with invalid dbtype " ++ show dbtype
-- | Create the migration plan for the given 'PersistEntity'
-- @val@.
migrate' :: [EntityDef]
         -> (Text -> IO Statement)
         -> EntityDef
         -> IO (Either [Text] [(Bool, Text)])
migrate' allDefs getter val = do
    let name = entityDB val
    (idClmn, old, mseq) <- getColumns getter val
    let (newcols, udefs, fdefs) = mkColumns allDefs val
    let udspair = map udToPair udefs
    let addSequence = AddSequence $ concat
            [ "CREATE SEQUENCE " 
            ,getSeqNameEscaped name
            , " START WITH 1 INCREMENT BY 1"
            ]
    case (idClmn, old, partitionEithers old, mseq) of
      -- Nothing found, create everything
      ([], [], _, _) -> do
        let idtxt = case entityPrimary val of
                      Just pdef -> " CONSTRAINT " <> escapeDBName (pkeyName (entityDB val)) <> " PRIMARY KEY (" <> (intercalate "," $ map (escapeDBName . fieldDB) $ compositeFields pdef) <> ")"
                      Nothing -> concat [escapeDBName $ fieldDB $ entityId val, " NUMBER NOT NULL PRIMARY KEY "]
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

        return $ Right $ map showAlterDb $ addTable : addSequence : uniques ++ foreigns ++ foreignsAlt
      -- No errors and something found, migrate
      (_, _, ([], old'),mseq') -> do
        let excludeForeignKeys (xs,ys) = (map (\c -> case cReference c of
                                                    Just (_,fk) -> case find (\f -> fk == foreignConstraintNameDBName f) fdefs of
                                                                     Just _ -> tracex ("\n\n\nremoving cos a composite fk="++show fk) $ 
                                                                                c { cReference = Nothing }
                                                                     Nothing -> c
                                                    Nothing -> c) xs,ys)
            (acs, ats) = getAlters allDefs name (newcols, udspair) $ excludeForeignKeys $ partitionEithers old'
            acs' = map (AlterColumn name) acs
            ats' = map (AlterTable  name) ats
        return $ Right $ map showAlterDb $ acs' ++ ats' ++ (maybe [addSequence] (const []) mseq')    
      -- Errors
      (_, _, (errs, _), _) -> return $ Left errs


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
             | AddSequence String

udToPair :: UniqueDef -> (DBName, [DBName])
udToPair ud = (uniqueDBName ud, map snd $ uniqueFields ud)

----------------------------------------------------------------------


-- | Returns all of the 'Column'@s@ in the given table currently
-- in the database.
getColumns :: (Text -> IO Statement)
           -> EntityDef
           -> IO ( [Either Text (Either Column (DBName, [DBName]))] -- ID column
                 , [Either Text (Either Column (DBName, [DBName]))] -- everything else
                 , Maybe PersistValue -- sequence name
                 )
getColumns getter def = do
    -- Find out ID column.
    stmtIdClmn <- getter $ T.concat 
                         ["SELECT COLUMN_NAME, "
                         ,"cast(NULLABLE as CHAR) as IS_NULLABLE, "
                         ,"DATA_TYPE, "
                         ,"DATA_DEFAULT as COLUMN_DEFAULT "
                         ,"FROM user_tab_cols "
                         ,"WHERE TABLE_NAME   = ? "
                         ,"AND COLUMN_NAME  = ?"]
    inter1 <- with (stmtQuery stmtIdClmn vals) (`connect` CL.consume)
    ids <- runResourceT $ CL.sourceList inter1 `connect` helperClmns -- avoid nested queries

    -- Find if sequence already exists.
    stmtSeq <- getter $ T.concat ["SELECT sequence_name "
                          ,"FROM user_sequences "
                          ,"WHERE sequence_name   = ?"]
    seqlist <- with (stmtQuery stmtSeq [PersistText $ getSeqNameUnescaped $ entityDB def]) (`connect` CL.consume)
    --liftIO $ putStrLn $ "seqlist=" ++ show seqlist

    -- Find out all columns.
    stmtClmns <- getter $ T.concat ["SELECT COLUMN_NAME, "
                                 ,"cast(NULLABLE as CHAR) as IS_NULLABLE, "
                                 ,"DATA_TYPE, "
                                 ,"DATA_DEFAULT "
                        ,"FROM user_tab_cols "
                          ,"WHERE TABLE_NAME   = ? "
                          ,"AND COLUMN_NAME <> ?"]
    inter2 <- liftIO $ with (stmtQuery stmtClmns vals) (`connect` CL.consume)
    cs <- runResourceT $ CL.sourceList inter2 `connect` helperClmns -- avoid nested queries

    -- Find out the constraints.    

    stmtCntrs <- getter $ T.concat 
      ["SELECT a.CONSTRAINT_NAME, "
      ,"a.COLUMN_NAME "
      ,"FROM user_cons_columns a,user_constraints b "
      ,"WHERE a.table_name = ? "
      ,"and a.table_name=b.table_name "
      ,"and a.constraint_name=b.constraint_name "
      ,"and b.constraint_type in ('U') "
      ,"AND a.COLUMN_NAME <> ? "
      ,"ORDER BY b.CONSTRAINT_NAME, "
      ,"a.COLUMN_NAME"]
    us <- with (stmtQuery stmtCntrs vals) (`connect` helperCntrs)

    -- Return both
    return (ids, cs ++ us, listAsMaybe seqlist)
  where
    listAsMaybe [] = Nothing
    listAsMaybe [[x]] = Just x
    listAsMaybe xs = error $ "returned to many sequences xs=" ++ show xs
    
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

      stmt <- lift $ getter $ T.concat 
        ["SELECT "
        ,"UCC.TABLE_NAME as REFERENCED_TABLE_NAME, "
        ,"UC.CONSTRAINT_NAME as CONSTRAINT_NAME "
        ,"FROM USER_CONSTRAINTS  UC, "
        ,"USER_CONS_COLUMNS UCC, "
        ,"USER_CONS_COLUMNS CC "
        ,"WHERE UC.R_CONSTRAINT_NAME = UCC.CONSTRAINT_NAME "
        ,"AND uc.constraint_type = 'R' "
        ,"and cc.constraint_name=uc.constraint_name "
        ,"and ucc.position = 1 "
        ,"and cc.position = 1 "
        ,"and uc.table_name=? "
        ,"and cc.column_name=? "
        ,"ORDER BY UC.TABLE_NAME, "
        ,"UC.R_CONSTRAINT_NAME, "
        ,"UCC.TABLE_NAME, "
        ,"UCC.COLUMN_NAME"]

      let vars = [ PersistText $ unDBName tname
                 , PersistByteString cname 
                 ]
      cntrs <- liftIO $ with (stmtQuery stmt vars) (`connect` CL.consume)
      ref <- case cntrs of
               [] -> return Nothing
               [[PersistByteString tab, PersistByteString ref]] ->
                   return $ Just (DBName $ T.decodeUtf8 tab, DBName $ T.decodeUtf8 ref)
               a1 -> fail $ "Oracle.getColumn/getRef: never here error[" ++ show a1 ++ "]"

      -- Okay!
      return Column
        { cName = DBName $ T.decodeUtf8 cname
        , cNull = null_ == "Y"
        , cSqlType = type_
        , cDefault = default_
        , cDefaultConstraintName = Nothing
        , cMaxLen = Nothing -- FIXME: maxLen
        , cReference = ref
        }

getColumn _ _ x =
    return $ Left $ pack $ "Invalid result from INFORMATION_SCHEMA: " ++ show x


-- | Parse the type of column as returned by Oracle's
-- schema tables.
parseType :: Monad m => ByteString -> m SqlType
parseType "CHAR"    = return SqlBool
-- Ints
--parseType "int"        = return SqlInt32
--parseType "short"      = return SqlInt32
--parseType "long"       = return SqlInt64
--parseType "longlong"   = return SqlInt64
--parseType "mediumint"  = return SqlInt32
parseType "NUMBER"     = return SqlInt32
-- **** todo parseType "number(a,b)"     = return SqlReal

-- Double
parseType "FLOAT"      = return SqlReal
parseType "DOUBLE"     = return SqlReal
--parseType "decimal"    = return SqlReal
--parseType "newdecimal" = return SqlReal
parseType "VARCHAR2(100)"    = return SqlString
-- Text
parseType "VARCHAR2"    = return SqlString
parseType "TEXT"       = return SqlString
--parseType "tinytext"   = return SqlString
--parseType "mediumtext" = return SqlString
parseType "LONG"   = return SqlString
-- ByteString
parseType "BLOB"       = return SqlBlob
--parseType "tinyblob"   = return SqlBlob
--parseType "mediumblob" = return SqlBlob
--parseType "longblob"   = return SqlBlob
-- Time-related
--parseType "time"       = return SqlTime
--parseType "datetime"   = return SqlDayTime
parseType "TIMESTAMP"  = return SqlDayTime
parseType "TIMESTAMP(6)"  = return SqlDayTime
--parseType "date"       = return SqlDay
--parseType "newdate"    = return SqlDay
--parseType "year"       = return SqlDay
-- Other
parseType b            = error $ "oracle: parseType no idea how to parse this b="++show b 


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
findAlters tblName allDefs col@(Column name isNull type_ def _defConstraintName _maxLen ref) cols =
    tracex ("\n\n\nfindAlters tablename="++show tblName++ " name="++ show name++" col="++show col++"\ncols="++show cols++"\n\n\n") $
      case filter ((name ==) . cName) cols of
        [] -> case ref of
               Nothing -> ([(name, Add' col)], [])
               Just (tname, b) -> let cnstr = tracex ("\n\ncols="++show cols++"\n\n2222findalters new foreignkey col["++showColumn col++"] name["++show name++"] tname["++show tname++"] b["++show b ++ "]") $ 
                                              [addReference allDefs (refName tblName name) tname name]
                                  in (map ((,) name) (Add' col : cnstr), cols)
        Column _ isNull' type_' def' _defConstraintName' _maxLen' ref':_ ->
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
                       | otherwise   = --tracex ("findAlters col=" ++ show col ++ " def=" ++ show def ++ " def'=" ++ show def') $
                                       case def of
                                         Nothing -> [(name, NoDefault)]
                                         Just s -> [(name, Default $ T.unpack s)]
            in ( refDrop ++ modType ++ modDef ++ refAdd
               , filter ((name /=) . cName) cols )


cmpdef::Maybe Text -> Maybe Text -> Bool
--cmpdef Nothing Nothing = True
cmpdef = (==)
--cmpdef (Just def) (Just def') = tracex ("def[" ++ show (T.concatMap (T.pack . show . ord) def) ++ "] def'[" ++ show (T.concatMap (T.pack . show . ord) def') ++ "]") $ def == def' 
--cmpdef _ _ = False

tpcheck :: SqlType -> SqlType -> Bool
tpcheck SqlInt32 SqlInt64 = True
tpcheck SqlInt64 SqlInt32 = True
tpcheck (SqlNumeric _ _) SqlInt32 = True -- else will try to migrate rational columns
tpcheck SqlInt32 (SqlNumeric _ _) = True
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
--    , case ref of
--        Nothing -> ""
--        Just (s, _) -> " REFERENCES " ++ escapeDBName s
    ]


-- | Renders an 'SqlType' in Oracle's format.
showSqlType :: SqlType
            -> Maybe Integer -- ^ @maxlen@
            -> String
showSqlType SqlBlob    Nothing    = "BLOB"
showSqlType SqlBlob    (Just _i)  = "BLOB" -- cannot specify the size 
showSqlType SqlBool    _          = "CHAR"
showSqlType SqlDay     _          = "DATE"
showSqlType SqlDayTime _          = "TIMESTAMP(6)"
--showSqlType SqlDayTimeZoned _     = "VARCHAR2(50)"
showSqlType SqlInt32   _          = "NUMBER"
showSqlType SqlInt64   _          = "NUMBER"
showSqlType SqlReal    _          = "FLOAT"
showSqlType (SqlNumeric s prec) _ = "NUMBER(" ++ show s ++ "," ++ show prec ++ ")"
showSqlType SqlString  Nothing    = "VARCHAR2(1000)"
showSqlType SqlString  (Just i)   = "VARCHAR2(" ++ show i ++ ")"
showSqlType SqlTime    _          = "TIME"
showSqlType (SqlOther t) _        = error ("oops in showSqlType " ++ show t)

-- | Render an action that must be done on the database.
showAlterDb :: AlterDB -> (Bool, Text)
showAlterDb (AddTable s) = (False, pack s)
showAlterDb (AddSequence s) = (False, pack s)
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
    , " DROP CONSTRAINT "
    , escapeDBName cname
    ]


-- | Render an action that must be done on a column.
showAlter :: DBName -> AlterColumn' -> String
showAlter table (_oldName, Change (Column n nu t def defConstraintName maxLen _ref)) =
    concat
    [ "ALTER TABLE "
    , escapeDBName table
    , " MODIFY ("
  --  , escapeDBName oldName
    , " "
    , showColumn (Column n nu t def defConstraintName maxLen Nothing)
    , ")"
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
    , " MODIFY ("
    , escapeDBName n
    , " DEFAULT "
    , s
    , ")"
    ]
showAlter table (n, NoDefault) =
    concat
    [ "ALTER TABLE "
    , escapeDBName table
    , " MODIFY "
    , escapeDBName n
    , " DEFAULT NULL"
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

-- ORA-00972: identifier is too long
refName :: DBName -> DBName -> DBName 
refName (DBName table) (DBName column) =
    DBName $ T.take 30 $ T.concat [table, "_", column, "_fkey"]

--refNames :: DBName -> [DBName] -> DBName
--refNames (DBName table) dbnames =
--    let columns = T.intercalate "_" $ map unDBName dbnames
--    in DBName $ T.take 30 $ T.concat [table, "_", columns, "_fkey"]

pkeyName :: DBName -> DBName
pkeyName (DBName table) =
    DBName $ T.take 30 $ T.concat [table, "_pkey"]

----------------------------------------------------------------------


-- | Escape a database name to be included on a query.
escapeDBName :: DBName -> String
escapeDBName (DBName s) = '"' : go (T.unpack s)
    where
      go ('"':xs) = '"' : '"' : go xs
      go ( x :xs) =     x     : go xs
      go ""       = "\""
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
    Nothing -> ISRInsertGet doInsert $ T.pack ("select cast(" ++ getSeqNameEscaped (entityDB ent) ++ ".currval as number) from dual")
     where
      doInsert = pack $ concat
        [ "INSERT INTO "
        , escapeDBName $ entityDB ent
        , "("
        , escapeDBName $ fieldDB $ entityId ent
        , if null (entityFields ent) then "" else ","
        , intercalate "," $ map (escapeDBName . fieldDB) $ entityFields ent
        , ") VALUES("
        , getSeqNameEscaped (entityDB ent) ++ ".nextval"
        , if null (entityFields ent) then "" else ","
        , intercalate "," (map (const "?") $ entityFields ent)
        , ")"
        ]

getSeqNameEscaped :: DBName -> String
getSeqNameEscaped d = escapeDBName $ DBName $ getSeqNameUnescaped d

getSeqNameUnescaped::DBName -> Text
getSeqNameUnescaped (DBName s) = "seq_" <> s <> "_id"

limitOffset::Bool -> (Int,Int) -> Bool -> Text -> Text 
limitOffset oracle12c' (limit,offset) hasOrder sql 
   | limit==0 && offset==0 = sql
   | oracle12c' && hasOrder && limit==0 = sql <> " offset " <> T.pack (show offset) <> " rows"
   | oracle12c' && hasOrder = sql <> " offset " <> T.pack (show offset) <> " rows fetch next " <> T.pack (show limit) <> " rows only"
   | otherwise = error $ "Oracle does not support limit and offset until Oracle 12c sql=" ++ T.unpack sql
