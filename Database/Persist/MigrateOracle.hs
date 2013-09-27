{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleContexts #-}
-- | A Oracle backend for @persistent@.
module Database.Persist.MigrateOracle
    ( migrateOracle
     ,insertSqlOracle
     ,escapeDBName
    ) where

import Control.Arrow
import Control.Monad.IO.Class (MonadIO (..))
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Error (ErrorT(..))
import Data.ByteString (ByteString)
import Data.Either (partitionEithers)
import Data.Function (on)
import Data.List (find, intercalate, sort, groupBy)
import Data.Text (Text, pack)

import Data.Conduit
import qualified Data.Conduit.List as CL
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import Data.Monoid ((<>))
import Database.Persist.Sql
-- | Create the migration plan for the given 'PersistEntity'
-- @val@.
migrateOracle :: Show a
         => [EntityDef a]
         -> (Text -> IO Statement)
         -> EntityDef SqlType
         -> IO (Either [Text] [(Bool, Text)])
migrateOracle allDefs getter val = do
    let name = entityDB val
    (idClmn, old) <- getColumns getter val
    let new = second (map udToPair) $ mkColumns allDefs val
    case (idClmn, old, partitionEithers old) of
      -- Nothing found, create everything
      ([], [], _) -> do
        let addTable = AddTable $ concat
                [ "CREATE TABLE "
                , escapeDBName name
                , "("
                , escapeDBName $ entityID val
                , " NUMBER NOT NULL PRIMARY KEY"
                , concatMap (\x -> ',' : showColumn x) $ fst new
                , ")"
                ]
        let addSequence = AddSequence $ concat
                [ "CREATE SEQUENCE " 
                ,getSeqNameEscaped name
                , " START WITH 1 INCREMENT BY 1"
                ]
        let uniques = flip concatMap (snd new) $ \(uname, ucols) ->
                      [ AlterTable name $
                        AddUniqueConstraint uname $
                        map (findTypeOfColumn allDefs name) ucols ]
        let foreigns = do
              Column cname _ _ _ _ (Just (refTblName, _)) <- fst new
              return $ AlterColumn name (cname, addReference allDefs refTblName)
        return $ Right $ map showAlterDb $ addTable : addSequence : uniques ++ foreigns
      -- No errors and something found, migrate
      (_, _, ([], old')) -> do
        let (acs, ats) = getAlters allDefs name new $ partitionEithers old'
            acs' = map (AlterColumn name) acs
            ats' = map (AlterTable  name) ats
        return $ Right $ map showAlterDb $ acs' ++ ats'
      -- Errors
      (_, _, (errs, _)) -> return $ Left errs


-- | Find out the type of a column.
findTypeOfColumn :: Show a => [EntityDef a] -> DBName -> DBName -> (DBName, FieldType)
findTypeOfColumn allDefs name col =
    maybe (error $ "Could not find type of column " ++
                   show col ++ " on table " ++ show name ++
                   " (allDefs = " ++ show allDefs ++ ")")
          ((,) col) $ do
            entDef   <- find ((== name) . entityDB) allDefs
            fieldDef <- find ((== col)  . fieldDB) (entityFields entDef)
            return (fieldType fieldDef)


-- | Helper for 'AddRefence' that finds out the 'entityID'.
addReference :: Show a => [EntityDef a] -> DBName -> AlterColumn
addReference allDefs name = AddReference name id_
    where
      id_ = maybe (error $ "Could not find ID of entity " ++ show name
                         ++ " (allDefs = " ++ show allDefs ++ ")")
                  id $ do
                    entDef <- find ((== name) . entityDB) allDefs
                    return (entityID entDef)

data AlterColumn = Change Column
                 | Add' Column
                 | Drop
                 | Default String
                 | NoDefault
                 | Update' String
                 | AddReference DBName DBName
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
           -> EntityDef a
           -> IO ( [Either Text (Either Column (DBName, [DBName]))] -- ID column
                 , [Either Text (Either Column (DBName, [DBName]))] -- everything else
                 )
getColumns getter def = do
    -- Find out ID column.
    stmtIdClmn <- getter "SELECT COLUMN_NAME, \
                                 \cast(NULLABLE as CHAR) as IS_NULLABLE, \
                                 \DATA_TYPE, \
                                 \DATA_DEFAULT as COLUMN_DEFAULT \
                          \FROM user_tab_cols \
                          \WHERE TABLE_NAME   = ? \
                            \AND COLUMN_NAME  = ?"
    inter1 <- runResourceT $ stmtQuery stmtIdClmn vals $$ CL.consume
    ids <- runResourceT $ CL.sourceList inter1 $$ helperClmns -- avoid nested queries

    -- Find out all columns.
    stmtClmns <- getter "SELECT COLUMN_NAME, \
                                 \cast(NULLABLE as CHAR) as IS_NULLABLE, \
                               \DATA_TYPE, \
                               \DATA_DEFAULT \
                        \FROM user_tab_cols \
                          \WHERE TABLE_NAME   = ? \
                          \AND COLUMN_NAME <> ?"
    inter2 <- runResourceT $ stmtQuery stmtClmns vals $$ CL.consume
    cs <- runResourceT $ CL.sourceList inter2 $$ helperClmns -- avoid nested queries

    -- Find out the constraints.    

    stmtCntrs <- getter "SELECT a.CONSTRAINT_NAME, \
                               \a.COLUMN_NAME \
      \FROM user_cons_columns a,user_constraints b \
     \WHERE a.table_name = ? \
     \and a.table_name=b.table_name \
     \and a.constraint_name=b.constraint_name \
     \and b.constraint_type in ('U','P') \
                          \AND a.COLUMN_NAME <> ? \
                        \ORDER BY b.CONSTRAINT_NAME, \
                                 \a.COLUMN_NAME"
    us <- runResourceT $ stmtQuery stmtCntrs vals $$ helperCntrs

    -- Return both
    return $ (ids, cs ++ us)
  where
    vals = [ PersistText $ unDBName $ entityDB def
           , PersistText $ unDBName $ entityID def ]

    helperClmns = CL.mapM getIt =$ CL.consume
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
    runErrorT $ do
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

      stmt <- lift $ getter "SELECT \
              \UCC.TABLE_NAME as REFERENCED_TABLE_NAME, \
              \UC.CONSTRAINT_NAME as CONSTRAINT_NAME \
            \FROM USER_CONSTRAINTS  UC, \
                 \USER_CONS_COLUMNS UCC, \
                 \USER_CONS_COLUMNS CC \
           \WHERE UC.R_CONSTRAINT_NAME = UCC.CONSTRAINT_NAME \
             \AND uc.constraint_type = 'R' \
             \and cc.constraint_name=uc.constraint_name \
             \and uc.table_name=? \
             \and cc.column_name=? \
           \ORDER BY UC.TABLE_NAME, \
             \UC.R_CONSTRAINT_NAME, \
             \UCC.TABLE_NAME, \
             \UCC.COLUMN_NAME"

      let vars = [ PersistText $ unDBName $ tname
                 , PersistByteString cname 
                 ]
      cntrs <- runResourceT $ stmtQuery stmt vars $$ CL.consume
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
parseType b            = error $ "oracle: parseType no idea how to parse this b="++show b -- return $ trace ("OOPS "++show b) $ SqlOther $ T.decodeUtf8 b


----------------------------------------------------------------------


-- | @getAlters allDefs tblName new old@ finds out what needs to
-- be changed from @old@ to become @new@.
getAlters :: Show a
          => [EntityDef a]
          -> DBName
          -> ([Column], [(DBName, [DBName])])
          -> ([Column], [(DBName, [DBName])])
          -> ([AlterColumn'], [AlterTable])
getAlters allDefs tblName (c1, u1) (c2, u2) =
    (getAltersC c1 c2, getAltersU u1 u2)
  where
    getAltersC [] old = concatMap dropColumn old
    getAltersC (new:news) old =
        let (alters, old') = findAlters allDefs new old
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
findAlters :: Show a => [EntityDef a] -> Column -> [Column] -> ([AlterColumn'], [Column])
findAlters allDefs col@(Column name isNull type_ def _maxLen ref) cols =
    case filter ((name ==) . cName) cols of
        [] -> ( let cnstr = [addReference allDefs tname | Just (tname, _) <- [ref]]
                in map ((,) name) (Add' col : cnstr)
              , cols )
        Column _ isNull' type_' def' _maxLen' ref':_ ->
            let -- Foreign key
                refDrop = case (ref == ref', ref') of
                            (False, Just (_, cname)) -> [(name, DropReference cname)]
                            _ -> []
                refAdd  = case (ref == ref', ref) of
                            (False, Just (tname, _)) -> [(name, addReference allDefs tname)]
                            _ -> []
                -- Type and nullability
                modType | tpcheck type_ type_' && isNull == isNull' = []
                        | otherwise = [(name, Change col)]
                -- Default value
                modDef | def == def' = []
                       | otherwise   = case def of
                                         Nothing -> [(name, NoDefault)]
                                         Just s -> [(name, Default $ T.unpack s)]
            in ( refDrop ++ modType ++ modDef ++ refAdd
               , filter ((name /=) . cName) cols )

tpcheck SqlInt32 SqlInt64 = True
tpcheck SqlInt64 SqlInt32 = True
tpcheck (SqlNumeric _ _) SqlInt32 = True -- else will try to migrate rational columns
tpcheck SqlInt32 (SqlNumeric _ _) = True
tpcheck a b = a==b

----------------------------------------------------------------------


-- | Prints the part of a @CREATE TABLE@ statement about a given
-- column.
showColumn :: Column -> String
showColumn (Column n nu t def maxLen ref) = concat
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
showSqlType SqlBlob    (Just i)   = "BLOB(" ++ show i ++ ")"
showSqlType SqlBool    _          = "CHAR"
showSqlType SqlDay     _          = "DATE"
showSqlType SqlDayTime _          = "TIMESTAMP(6)"
showSqlType SqlDayTimeZoned _     = "VARCHAR2(50)"
showSqlType SqlInt32   _          = "NUMBER"
showSqlType SqlInt64   _          = "NUMBER"
showSqlType SqlReal    _          = "FLOAT"
showSqlType (SqlNumeric s prec) _ = "NUMBER(" ++ show s ++ "," ++ show prec ++ ")"
showSqlType SqlString  Nothing    = "VARCHAR2(1000)"
showSqlType SqlString  (Just i)   = "VARCHAR2(" ++ show i ++ ")"
showSqlType SqlTime    _          = "TIME"
showSqlType (SqlOther t) _        = error ("oops in showSqlType " ++ show t)  -- $ T.unpack t

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
    where
      escapeDBName' (name, (FTTypeCon _ "Text"      )) = escapeDBName name ++ "(200)"
      escapeDBName' (name, (FTTypeCon _ "String"    )) = escapeDBName name ++ "(200)"
      escapeDBName' (name, (FTTypeCon _ "ByteString")) = escapeDBName name ++ "(200)"
      escapeDBName' (name, _                       ) = escapeDBName name
showAlterTable table (DropUniqueConstraint cname) = concat
    [ "ALTER TABLE "
    , escapeDBName table
    , " DROP CONSTRAINT "
    , escapeDBName cname
    ]


-- | Render an action that must be done on a column.
showAlter :: DBName -> AlterColumn' -> String
showAlter table (oldName, Change (Column n nu t def maxLen _ref)) =
    concat
    [ "ALTER TABLE "
    , escapeDBName table
    , " MODIFY ("
  --  , escapeDBName oldName
    , " "
    , showColumn (Column n nu t def maxLen Nothing)
    , ")"
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
showAlter table (n, AddReference t2 id2) = concat
    [ "ALTER TABLE "
    , escapeDBName table
    , " ADD CONSTRAINT "
    , escapeDBName $ refName table n
    , " FOREIGN KEY("
    , escapeDBName n
    , ") REFERENCES "
    , escapeDBName t2
    , "("
    , escapeDBName id2
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
escapeDBName (DBName s) = '"' : go (T.unpack s)
    where
      go ('"':xs) = '"' : '"' : go xs
      go ( x :xs) =     x     : go xs
      go ""       = "\""
-- | SQL code to be executed when inserting an entity.
insertSqlOracle :: DBName -> [DBName] -> DBName -> InsertSqlResult
insertSqlOracle t cols idcol = ISRInsertGet doInsert $ T.pack ("select cast(" ++ getSeqNameEscaped t ++ ".currval as number) from dual")
    where
      doInsert = pack $ concat
        [ "INSERT INTO "
        , escapeDBName t
        , "("
        , escapeDBName idcol
        , (if null cols then "" else ",")
        , intercalate "," $ map escapeDBName cols
        , ") VALUES("
        , getSeqNameEscaped t ++ ".nextval"
        , (if null cols then "" else ",")
        , intercalate "," (map (const "?") cols)
        , ")"
        ]

getSeqNameEscaped :: DBName -> String
getSeqNameEscaped (DBName s) = escapeDBName $ DBName ("seq_" <> s <> "_id")