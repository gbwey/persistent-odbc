-- stack build --test --flag persistent-odbc:tester --stack-yaml=stack865.yaml
-- stack --stack-yaml=stack865.yaml exec -- testodbc s
-- stack build --test --flag persistent-odbc:tester --stack-yaml=stack8103.yaml
-- stack --stack-yaml=stack8103.yaml exec -- testodbc s
{-# OPTIONS -Wall #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE EmptyDataDecls #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE StandaloneDeriving #-}
module Main where
import Database.Persist
import Database.Persist.ODBC
import Database.Persist.TH
import Control.Monad.Trans.Reader (ask)
import Control.Monad.Trans.Resource (runResourceT, ResourceT)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Logger
import qualified Data.Text as T
import Data.Functor
import Data.Time (getCurrentTime,UTCTime)

import System.Environment (getArgs)
import Employment
import Data.Conduit
import qualified Data.Conduit.List as CL
import Control.Monad (when,unless,forM_)
import qualified Database.HDBC as H
import qualified Database.HDBC.ODBC as H

import FtypeEnum
import qualified Database.Esqueleto as E
import Database.Esqueleto (select,where_,(^.),from,Value(..))

import Data.ByteString (ByteString)
import Data.Ratio
import Text.Blaze.Html
--import Debug.Trace

share [mkPersist sqlSettings, mkMigrate "migrateAll", mkDeleteCascade sqlSettings] [persistLowerCase|
Test0
    mybool Bool
    deriving Show
Test1
    flag Bool
    flag1 Bool Maybe
    dbl Double
    db2 Double Maybe
    deriving Show

Test2
    dt UTCTime
    deriving Show

Persony
    name String
    employment Employment
    deriving Show

Personx
    name String Eq Ne Desc
    age Int Lt Asc
    color String Maybe Eq Ne
    Unique PersonxNameKey name
    deriving Show

Testnum
    bar Int
    znork Int Maybe
    znork1 String
    znork2 String Maybe
    znork3 UTCTime
    name String Maybe
    deriving Show
Foo
    bar String
    deriving Show
Personz
    name String
    age Int Maybe
    deriving Show
BlogPost
    title String
    refPersonz PersonzId
    deriving Show

Asm
  name String
  description String
  Unique MyUniqueAsm name
  deriving Show

Xsd
  name String
  description String
  asmid AsmId
  Unique MyUniqueXsd name -- global
  deriving Show

Ftype
  name String
  Unique MyUniqueFtype name
  deriving Show

Line
  name String
  description String
  pos Int
  ftypeid FtypeEnum
  xsdid XsdId
  Unique EinzigName xsdid name  -- within an xsd:so can repeat fieldnames in different xsds
  Unique EinzigPos xsdid pos
  deriving Show


Interface
  name String
  fname String
  ftypeid FtypeId
  iname FtypeEnum
  Unique MyUniqueInterface name
  deriving Show

Testother -- json -- no FromJSON instance in aeson anymore for bytestring
  bs1 ByteString Maybe
  bs2 ByteString
  deriving Show

Testrational json
  rat Rational
  deriving Show

Testhtml
  myhtml Html
  -- deriving Show

Testblob
  bs1 ByteString Maybe
  deriving Show

Testblob3
  bs1 ByteString
  bs2 ByteString
  bs3 ByteString
  deriving Show

Testlen
  txt  String maxlen=5 -- default='xx12'
  str  String maxlen=5
  bs   ByteString maxlen=5
  mtxt String Maybe maxlen=5
  mstr String Maybe maxlen=5
  mbs  ByteString Maybe maxlen=5
  deriving Show

Aaaa
  name String maxlen=100
  deriving Show Eq

Bbbb
  name String maxlen=100
  deriving Show Eq

Both
  refAaaa AaaaId
  refBbbb BbbbId
  Primary refAaaa refBbbb
  deriving Show Eq
|]

main :: IO ()
main = do
  [arg] <- getArgs
  let (dbtype',dsn) =
       case arg of -- odbc system dsn
           "d" -> (DB2,"dsn=db2_test")
           "p" -> (Postgres,"dsn=pg_test")
           "m" -> (MySQL,"dsn=mysql_test")
-- have to pass UID=..; PWD=..; or use Trusted_Connection or Trusted Connection depending on the driver and environment
           "s" -> (MSSQL True,"dsn=mssql_test") -- mssql 2012 [full limit and offset support]
           "so" -> (MSSQL False,"dsn=mssql_test") -- mssql pre 2012 [limit support only]
           "o" -> (Oracle False,"dsn=oracle_test") -- pre oracle 12c [no support for limit and offset]
           "on" -> (Oracle True,"dsn=oracle_test") -- >= oracle 12c [full limit and offset support]
           "q" -> (Sqlite False,"dsn=sqlite_test;NoWCHAR=1")
           "qn" -> (Sqlite True,"dsn=sqlite_test;NoWCHAR=1")
           xs -> error $ "unknown option:choose p m s so o on d q qn found[" ++ xs ++ "]"

  runResourceT $ runNoLoggingT $ withODBCConn Nothing dsn $ runSqlConn $ do
    conn <- ask
    let dbtype=read $ T.unpack $ connRDBMS conn
    liftIO $ putStrLn $ "original:" ++ show dbtype' ++ " calculated:" ++ show dbtype
    liftIO $ putStrLn "\nbefore migration\n"
    runMigration migrateAll
    liftIO $ putStrLn "after migration"
    case dbtype of
      MSSQL {} -> do -- deleteCascadeWhere Asm causes seg fault for mssql only
          deleteWhere ([] :: [Filter Line])
          deleteWhere ([] :: [Filter Xsd])
          deleteWhere ([] :: [Filter Asm])
      _ -> do
          deleteCascadeWhere ([] :: [Filter Asm])
          deleteCascadeWhere ([] :: [Filter Personz])

    deleteWhere ([] :: [Filter Personz])
    deleteWhere ([] :: [Filter Persony])
    deleteWhere ([] :: [Filter Personx])
    deleteWhere ([] :: [Filter Testother])
    deleteWhere ([] :: [Filter Testrational])
    deleteWhere ([] :: [Filter Testblob])
    deleteWhere ([] :: [Filter Testblob3])
    deleteWhere ([] :: [Filter Testother])
    deleteWhere ([] :: [Filter Testnum])
    deleteWhere ([] :: [Filter Testhtml])
    deleteWhere ([] :: [Filter Testblob3])
    deleteWhere ([] :: [Filter Test1])
    deleteWhere ([] :: [Filter Test0])
    deleteWhere ([] :: [Filter Testlen])

    when True $ testbase dbtype
    liftIO $ putStrLn "Ended tests"


testbase :: DBType -> SqlPersistT (NoLoggingT (ResourceT IO)) ()
testbase dbtype = do
    liftIO $ putStrLn "\n*** in testbase\n"
    a1 <- insert $ Foo "test"
    liftIO $ putStrLn $ "a1=" ++ show a1
    a2 <- selectList ([] :: [Filter Foo]) []
    liftIO $ putStrLn $ "a2="
    liftIO $ mapM_ print a2

    johnId <- insert $ Personz "John Doe" $ Just 35
    liftIO $ putStrLn $ "johnId[" ++ show johnId ++ "]"
    janeId <- insert $ Personz "Jane Doe" Nothing
    liftIO $ putStrLn $ "janeId[" ++ show janeId ++ "]"

    a3 <- selectList ([] :: [Filter Personz]) []
    unless (length a3 == 2) $ error $ "wrong number of Personz rows " ++ show a3
    liftIO $ putStrLn $ "a3="
    liftIO $ mapM_ print a3


    void $ insert $ BlogPost "My fr1st p0st" johnId
    liftIO $ putStrLn $ "after insert johnId"
    void $ insert $ BlogPost "One more for good measure" johnId
    liftIO $ putStrLn $ "after insert johnId 2"
    a4 <- selectList ([] :: [Filter BlogPost]) []
    liftIO $ putStrLn $ "a4="
    liftIO $ mapM_ print a4
    unless (length a4 == 2) $ error $ "wrong number of BlogPost rows " ++ show a4

    --oneJohnPost <- selectList [BlogPostAuthorId ==. johnId] [LimitTo 1]
    --liftIO $ print (oneJohnPost :: [Entity BlogPost])

    john <- get johnId
    liftIO $ print (john :: Maybe Personz)
    dt <- liftIO getCurrentTime
    v1 <- insert $ Testnum 100 Nothing "hello" (Just "world") dt Nothing
    v2 <- insert $ Testnum 100 (Just 222) "dude" Nothing dt (Just "something")
    liftIO $ putStrLn $ "v1=" ++ show v1
    liftIO $ putStrLn $ "v2=" ++ show v2

    a5 <- selectList ([] :: [Filter Testnum]) []
    unless (length a5 == 2) $ error $ "wrong number of Testnum rows " ++ show a5

    delete janeId
    deleteWhere [BlogPostRefPersonz ==. johnId]
    test0
    test1 dbtype
    test2
    test3
    test4
    case dbtype of
      MSSQL {} -> return ()
      _ -> test5 dbtype
    test6
    when (limitoffset dbtype) test7
    when (limitoffset dbtype) test8

    case dbtype of
      Oracle { oracle12c=False } -> return ()
      _ -> test9

    case dbtype of
      MSSQL {} -> return ()
      _ -> test10 dbtype

    case dbtype of
      MSSQL {} -> return ()
      _ -> test11 dbtype
    test12 dbtype

test0 :: SqlPersistT (NoLoggingT (ResourceT IO)) ()
test0 = do
    liftIO $ putStrLn "\n*** in test0\n"
    pid <- insert $ Personx "Michael" 25 Nothing
    liftIO $ print pid

    p1 <- get pid
    liftIO $ print p1

    replace pid $ Personx "Michael" 26 Nothing
    p2 <- get pid
    liftIO $ print p2

    p3 <- selectList [PersonxName ==. "Michael"] []
    liftIO $ mapM_ print p3

    _ <- insert $ Personx "Michael2" 27 Nothing
    deleteWhere [PersonxName ==. "Michael2"]
    p4 <- selectList [PersonxAge <. 28] []
    liftIO $ mapM_ print p4

    update pid [PersonxAge =. 28]
    p5 <- get pid
    liftIO $ print p5

    updateWhere [PersonxName ==. "Michael"] [PersonxAge =. 29]
    p6 <- get pid
    liftIO $ print p6

    _ <- insert $ Personx "Eliezer" 2 $ Just "blue"
    p7 <- selectList [] [Asc PersonxAge]
    liftIO $ mapM_ print p7

    _ <- insert $ Personx "Abe" 30 $ Just "black"
    p8 <- selectList [PersonxAge <. 30] [Desc PersonxName]
    liftIO $ mapM_ print p8

    _ <- insert $ Personx "Abe1" 31 $ Just "brown"
    p9 <- selectList [PersonxName ==. "Abe1"] []
    liftIO $ mapM_ print p9

    a6 <- selectList ([] :: [Filter Personx]) []
    unless (length a6 == 4) $ error $ "wrong number of Personx rows " ++ show a6

    p10 <- getBy $ PersonxNameKey "Michael"
    liftIO $ print p10

    p11 <- selectList [PersonxColor ==. Just "blue"] []
    liftIO $ mapM_ print p11

    p12 <- selectList [PersonxColor ==. Nothing] []
    liftIO $ mapM_ print p12

    p13 <- selectList [PersonxColor !=. Nothing] []
    liftIO $ mapM_ print p13

    delete pid
    plast <- get pid
    liftIO $ print plast

test1 :: DBType -> SqlPersistT (NoLoggingT (ResourceT IO)) ()
test1 dbtype = do
    liftIO $ putStrLn "\n*** in test1\n"
    pid1 <- insert $ Persony "Dude" Retired
    liftIO $ print pid1
    pid2 <- insert $ Persony "Dude1" Employed
    liftIO $ print pid2
    pid3 <- insert $ Persony "Snoyman aa" Unemployed
    liftIO $ print pid3
    pid4 <- insert $ Persony "bbb Snoyman" Employed
    liftIO $ print pid4

    a1 <- selectList ([] :: [Filter Persony]) []
    unless (length a1 == 4) $ error $ "wrong number of Personz rows " ++ show a1
    liftIO $ putStrLn $ "persony "
    liftIO $ mapM_ print a1

    let sql = case dbtype of
                MSSQL {} -> "SELECT [name] FROM [persony] WHERE [name] LIKE '%Snoyman%'"
                MySQL {} -> "SELECT `name` FROM `persony` WHERE `name` LIKE '%Snoyman%'"
                _        -> "SELECT \"name\" FROM \"persony\" WHERE \"name\" LIKE '%Snoyman%'"
    runConduit $ rawQuery sql [] .| CL.mapM_ (liftIO . print)

test2 :: SqlPersistT (NoLoggingT (ResourceT IO)) ()
test2 = do
    liftIO $ putStrLn "\n*** in test2\n"
    aaa <- insert $ Test0 False
    liftIO $ print aaa

    a1 <- selectList ([] :: [Filter Test0]) []
    unless (length a1 == 1) $ error $ "wrong number of Personz rows " ++ show a1

test3 :: SqlPersistT (NoLoggingT (ResourceT IO)) ()
test3 = do
    liftIO $ putStrLn "\n*** in test3\n"
    a1 <- insert $ Test1 True (Just False) 100.3 Nothing
    liftIO $ putStrLn $ "a1=" ++ show a1
    a2 <- insert $ Test1 False Nothing 100.3 (Just 12.44)
    liftIO $ putStrLn $ "a2=" ++ show a2
    a3 <- insert $ Test1 True (Just True) 100.3 (Just 11.11)
    liftIO $ putStrLn $ "a3=" ++ show a3
    a4 <- insert $ Test1 False Nothing 100.3 Nothing
    liftIO $ putStrLn $ "a4=" ++ show a4
    ret <- selectList ([] :: [Filter Test1]) []
    liftIO $ mapM_ print ret

    a5 <- selectList ([] :: [Filter Test1]) []
    unless (length a5 == 4) $ error $ "wrong number of Test1 rows " ++ show a5

test4 :: SqlPersistT (NoLoggingT (ResourceT IO)) ()
test4 = do
    liftIO $ putStrLn "\n*** in test4\n"
    a1 <- insert $ Asm "NewAsm1" "description for newasm1"

    x11 <- insert $ Xsd "NewXsd11" "description for newxsd11" a1
    liftIO $ putStrLn $ "x11=" ++ show x11
    l111 <- insert $ Line "NewLine111" "description for newline111" 10 Xsd_string x11
    liftIO $ putStrLn $ "l111=" ++ show l111
    l112 <- insert $ Line "NewLine112" "description for newline112" 11 Xsd_boolean x11
    liftIO $ putStrLn $ "l112=" ++ show l112
    l113 <- insert $ Line "NewLine113" "description for newline113" 12 Xsd_decimal x11
    liftIO $ putStrLn $ "l113=" ++ show l113
    l114 <- insert $ Line "NewLine114" "description for newline114" 15 Xsd_int x11
    liftIO $ putStrLn $ "l114=" ++ show l114

    x12 <- insert $ Xsd "NewXsd12" "description for newxsd12" a1
    liftIO $ putStrLn $ "x12=" ++ show x12
    l121 <- insert $ Line "NewLine121" "description for newline1" 12 Xsd_int x12
    liftIO $ putStrLn $ "l121=" ++ show l121
    l122 <- insert $ Line "NewLine122" "description for newline2" 19 Xsd_boolean x12
    liftIO $ putStrLn $ "l122=" ++ show l122
    l123 <- insert $ Line "NewLine123" "description for newline3" 13 Xsd_string x12
    liftIO $ putStrLn $ "l123=" ++ show l123
    l124 <- insert $ Line "NewLine124" "description for newline4" 99 Xsd_double x12
    liftIO $ putStrLn $ "l124=" ++ show l124
    l125 <- insert $ Line "NewLine125" "description for newline5" 2 Xsd_boolean x12
    liftIO $ putStrLn $ "l125=" ++ show l125

    a2 <- insert $ Asm "NewAsm2" "description for newasm2"
    liftIO $ putStrLn $ "a2=" ++ show a2

    a3 <- insert $ Asm "NewAsm3" "description for newasm3"
    liftIO $ putStrLn $ "a3=" ++ show a3
    x31 <- insert $ Xsd "NewXsd31" "description for newxsd311" a3
    liftIO $ putStrLn $ "x31=" ++ show x31

    a4 <- selectList ([] :: [Filter Asm]) []
    liftIO $ putStrLn "a4="
    liftIO $ mapM_ print a4

    unless (length a4 == 3) $ error $ "wrong number of Asm rows " ++ show a4
    a5 <- selectList ([] :: [Filter Xsd]) []
    liftIO $ putStrLn "a5="
    liftIO $ mapM_ print a5
    unless (length a5 == 3) $ error $ "wrong number of Xsd rows " ++ show a5
    a6 <- selectList ([] :: [Filter Line]) []
    liftIO $ putStrLn "a6="
    liftIO $ mapM_ print a6
    unless (length a6 == 9) $ error $ "wrong number of Line rows " ++ show a6

    [Value mpos] <- select $
                       from $ \ln -> do
                          where_ (ln ^. LineXsdid E.==. E.val x11)
                          return $ E.joinV $ E.max_ (E.just (ln ^. LinePos))
    liftIO $ putStrLn $ "mpos=" ++ show mpos

test5 :: DBType -> SqlPersistT (NoLoggingT (ResourceT IO)) ()
test5 dbtype = do
    liftIO $ putStrLn "\n*** in test5\n"
    a1 <- insert $ Testother (Just "abc") "zzzz"
    liftIO $ putStrLn $ "a1=" ++ show a1
    case dbtype of
      DB2 {} -> liftIO $ putStrLn $ show dbtype ++ " insert multiple blob fields with a null fails"
      _ -> do
              a2 <- insert $ Testother Nothing "aaa"
              liftIO $ putStrLn $ "a2=" ++ show a2
    a3 <- insert $ Testother (Just "nnn") "bbb"
    liftIO $ putStrLn $ "a3=" ++ show a3
    a4 <- insert $ Testother (Just "ddd") "mmm"
    liftIO $ putStrLn $ "a4=" ++ show a4
    xs <- case dbtype of
            Oracle {} -> selectList ([] :: [Filter Testother]) [] -- cannot sort blobs in oracle
            DB2 {} -> selectList ([] :: [Filter Testother]) [] -- cannot sort blobs in db2?
            _ -> selectList [] [Desc TestotherBs1]
    liftIO $ putStrLn $ "xs=" ++ show xs
    case dbtype of
      Oracle {} -> return ()
      DB2 {} -> return ()
      _ -> do
              ys <- selectList [] [Desc TestotherBs2]
              liftIO $ putStrLn $ "ys=" ++ show ys

    a7 <- selectList ([] :: [Filter Testother]) []
    case dbtype of
      DB2 {} -> unless (length a7 == 3) $ error $ show dbtype ++ " :wrong number of Testother rows " ++ show a7
      _ -> unless (length a7 == 4) $ error $ "wrong number of Testother rows " ++ show a7

    liftIO $ putStrLn "end of test5"

test6 :: SqlPersistT (NoLoggingT (ResourceT IO)) ()
test6  = do
    liftIO $ putStrLn "\n*** in test6\n"
    r1 <- insert $ Testrational (4%6)
    r2 <- insert $ Testrational (13 % 14)
    liftIO $ putStrLn $ "r1=" ++ show r1
    liftIO $ putStrLn $ "r2=" ++ show r2
    zs <- selectList [] [Desc TestrationalRat]
    liftIO $ putStrLn "zs="
    liftIO $ mapM_ print zs
    h1 <- insert $ Testhtml $ preEscapedToMarkup ("<p>hello</p>"::String)
    liftIO $ putStrLn $ "h1=" ++ show h1

    a1 <- selectList ([] :: [Filter Testrational]) []
    unless (length a1 == 2) $ error $ "wrong number of Testrational rows " ++ show a1

    a2 <- selectList ([] :: [Filter Testhtml]) []
    unless (length a2 == 1) $ error $ "wrong number of Testhtml rows "

test7 :: SqlPersistT (NoLoggingT (ResourceT IO)) ()
test7 = do
    liftIO $ putStrLn "\n*** in test7\n"
    a1 <- selectList [] [Desc LinePos, LimitTo 2, OffsetBy 3]
    liftIO $ putStrLn $ show (length a1) ++ " rows: limit=2,offset=3 a1="
    liftIO $ mapM_ print a1

    a2 <- selectList [] [Desc LinePos, LimitTo 2]
    liftIO $ putStrLn $ show (length a2) ++ " rows: limit=2 a2="
    liftIO $ mapM_ print a2

    a3 <- selectList [] [Desc LinePos, OffsetBy 3]
    liftIO $ putStrLn $ show (length a3) ++ " rows: offset=3 a3="
    liftIO $ mapM_ print a3


test8 :: SqlPersistT (NoLoggingT (ResourceT IO)) ()
test8 = do
    liftIO $ putStrLn "\n*** in test8\n"
    xs <- select $
             from $ \ln -> do
                where_ (ln ^. LinePos E.>=. E.val 0)
                E.orderBy [E.asc (ln ^. LinePos)]
                E.limit 2
                E.offset 3
                return ln
    liftIO $ putStrLn $ show (length xs) ++ " rows: limit=2 offset=3 xs=" ++ show xs

test9 :: SqlPersistT (NoLoggingT (ResourceT IO)) ()
test9 = do
    liftIO $ putStrLn "\n*** in test9\n"
    a1 <- selectList [] [Desc LinePos, LimitTo 2]
    liftIO $ putStrLn $ show (length a1) ++ " rows: limit=2,offset=0 a1="
    liftIO $ mapM_ print a1

    a2 <- selectList [] [Desc LinePos, LimitTo 4]
    liftIO $ putStrLn $ show (length a2) ++ " rows: limit=4,offset=0 a2="
    liftIO $ mapM_ print a2


test10 :: DBType -> SqlPersistT (NoLoggingT (ResourceT IO)) ()
test10 dbtype = do
    liftIO $ putStrLn "\n*** in test10\n"
    a1 <- insert $ Testblob3 "abc1" "def1" "zzzz1"
    liftIO $ putStrLn $ "a1=" ++ show a1
    a2 <- insert $ Testblob3 "abc2" "def2" "test2"
    liftIO $ putStrLn $ "a2=" ++ show a2
    case dbtype of
      Oracle {} -> liftIO $ putStrLn "skipping insert empty string into oracle blob column (treated as a null)"
        {-
        *** Exception: SqlError {seState = "[\"HY000\"]", seNativeError = -1,
        seErrorMsg = "execute execute: [\"1400: [Oracle][ODBC][Ora]ORA-01400: cannot insert NULL into (\\\"SYSTEM\\\".\\\"testblob3\\\".\\\"bs1\\\")\\n\"]"}
        -}
      _ -> void $ insert $ Testblob3 "" "hello3" "world3"
    ys <- selectList ([] :: [Filter Testblob3]) []
    liftIO $ putStrLn "ys="
    liftIO $ mapM_ print ys

    a3 <- selectList ([] :: [Filter Testblob3]) []
    case dbtype of
      Oracle {} -> unless (length a3 == 2) $ error $ show dbtype ++ " :wrong number of Testblob3 rows " ++ show a3
      _ -> unless (length a3 == 3) $ error $ "wrong number of Testblob3 rows " ++ show a3
    liftIO $ mapM_ print a3

test11 :: DBType -> SqlPersistT (NoLoggingT (ResourceT IO)) ()
test11 dbtype = do
    liftIO $ putStrLn "\n*** in test11\n"
    case dbtype of
       MSSQL {} -> liftIO $ putStrLn $ show dbtype ++ ":inserting null in a blob not supported so skipping"
       DB2 {} -> liftIO $ putStrLn $ show dbtype ++ ":inserting null in a blob not supported so skipping"
       _ -> do
              _ <- insert $ Testblob Nothing
              return ()
    _ <- insert $ Testblob $ Just "some data for testing"
    _ <- insert $ Testblob $ Just "world"
    liftIO $ putStrLn "after testblob inserts"

    xs <- selectList ([] :: [Filter Testblob]) [] -- mssql fails if there is a null in a blog column
    liftIO $ putStrLn $ "testblob xs="
    liftIO $ mapM_ print xs

    a1 <- selectList ([] :: [Filter Testblob]) []
    case dbtype of
      MSSQL {} -> unless (length a1 == 2) $ error $ show dbtype ++ " :wrong number of Testblob rows " ++ show a1
      DB2 {} -> unless (length a1 == 2) $ error $ show dbtype ++ " :wrong number of Testblob rows " ++ show a1
      _ -> unless (length a1 == 3) $ error $ "wrong number of Testblob rows " ++ show a1

test12 :: DBType -> SqlPersistT (NoLoggingT (ResourceT IO)) ()
test12 dbtype = do
    liftIO $ putStrLn "\n*** in test12\n"
    a1 <- insert $ Testlen "txt1" "str1" "bs1" (Just "txt1m") (Just "str1m") (Just "bs1m")
    liftIO $ putStrLn $ "a1=" ++ show a1
    a2 <- insert $ Testlen "txt2" "str2" "bs2" (Just "aaaa") (Just "str2m") (Just "bs2m")
    liftIO $ putStrLn $ "a2=" ++ show a2
    a3 <- selectList ([] :: [Filter Testlen]) []
    case dbtype of
      -- Oracle {} -> unless (length a3 == 2) $ error $ show dbtype ++ " :wrong number of Testlen rows " ++ show a3
      _ -> unless (length a3 == 2) $ error $ "wrong number of Testlen rows " ++ show a3

    liftIO $ putStrLn $ "a3="
    liftIO $ mapM_ print a3

limitoffset :: DBType -> Bool
limitoffset dbtype = -- trace ("limitoffset dbtype=" ++ show dbtype) $
  case dbtype of
    Oracle False -> False
    MSSQL False -> False
    _ -> True

main2 :: IO ()
main2 = do
--  let connectionString = "dsn=mssql_test; Trusted_Connection=True"
  let connectionString = "dsn=db2_test"
  conn <- H.connectODBC connectionString
  putStrLn "\n1\n"
  stmt1 <- H.prepare conn "select * from test93"
  putStrLn "\n2\n"
  vals1 <- H.execute stmt1 []
  print vals1
  putStrLn "\n3\n"
  results1 <- H.fetchAllRowsAL stmt1
  putStrLn "\n4\n"
  forM_ (zip [1::Int ..] results1) $ \(i,x) -> putStrLn $ "i=" ++ show i ++ " result=" ++ show x

  putStrLn "\na\n"
  --stmt1 <- H.prepare conn "create table test93 (bs1 blob)"
  --putStrLn "\nb\n"
  --vals <- H.execute stmt1 []
  --putStrLn "\nc\n"
  stmt2 <- H.prepare conn "insert into test93 values(blob(?))"
  putStrLn "\nd\n"
  vals2 <- H.execute stmt2 [H.SqlByteString "hello world"]
  putStrLn "\ne\n"
  print vals2
--  _ <- H.commit conn
--  error "we are done!!"


--  a <- H.quickQuery' conn "select * from testblob" []  -- hangs here in both mssql drivers [not all the time]
--  putStrLn $ "\n5\n"
--  print a

  stmt3 <- H.prepare conn "insert into testblob (bs1) values(?)"
  putStrLn "\n6\n"
  vals3a <- H.execute stmt3 [H.SqlNull]
  putStrLn $ "vals3a=" ++ show vals3a
  putStrLn "\n7\n"
  vals3b <- H.execute stmt3 [H.SqlNull]
  putStrLn $ "vals3b=" ++ show vals3b
  putStrLn "\n8\n"
  vals3c <- H.execute stmt3 [H.SqlNull]
  putStrLn $ "vals3c=" ++ show vals3c
  putStrLn "\n9\n"

  results2 <- H.fetchAllRowsAL stmt2
  forM_ (zip [1::Int ..] results2) $ \(i,x) -> putStrLn $ "i=" ++ show i ++ " result=" ++ show x
  putStrLn "\nTESTBLOB worked\n"

  --stmt <- H.prepare conn "insert into testother (bs1,bs2) values(convert(varbinary(max),?),convert(varbinary(max),?))"
  stmt4 <- H.prepare conn "insert into testother (bs1,bs2) values(convert(varbinary(max), cast (? as varchar(100))),convert(varbinary(max), cast (? as varchar(100))))"
  vals4 <- H.execute stmt4 [H.SqlByteString "hello",H.SqlByteString "test"]
  putStrLn $ "vals4=" ++ show vals4

  --vals <- H.execute stmt3 [H.SqlNull,H.SqlByteString "test"]
  putStrLn "\nTESTOTHER worked\n"

  H.commit conn


main3 :: IO ()
main3 = do
  --let connectionString = "dsn=pg_test"
  let connectionString = "dsn=mysql_test"
  --let connectionString = "dsn=mssql_test; Trusted_Connection=True"
  --let connectionString = "dsn=oracle_test"
  --let connectionString = "dsn=db2_test"
  conn <- H.connectODBC connectionString
  putStrLn "\n1\n"

  putStrLn $ "drivername=" ++ H.hdbcDriverName conn
  putStrLn $ "clientver=" ++ H.hdbcClientVer conn
  putStrLn $ "proxied drivername=" ++ H.proxiedClientName conn
  putStrLn $ "proxied clientver=" ++ H.proxiedClientVer conn
  putStrLn $ "serverver=" ++ H.dbServerVer conn
  let lenfn="len" -- mssql
  --let lenfn="length"
  a <- H.describeTable conn "persony"
  print a
  stmt1 <- H.prepare conn ("update \"persony\" set \"name\" = ? where \"id\" >= ?")
  putStrLn "\n2\n"
  vals1 <- H.execute stmt1 [H.SqlString "dooble and stuff", H.toSql (1 :: Integer)]
  print vals1
  putStrLn "\n3\n"
  stmt2 <- H.prepare conn ("select \"id\","++lenfn++"(\"name\"),\"name\" from \"persony\"")
  putStrLn "\n4\n"
  vals2 <- H.execute stmt2 []
  print vals2
  results <- H.fetchAllRowsAL' stmt2
  mapM_ print results
  H.commit conn

main4 :: IO ()
main4 = do
  --let connectionString = "dsn=pg_test"
  let connectionString = "dsn=mysql_test"
  --let connectionString = "dsn=mssql_test; Trusted_Connection=True"
  --let connectionString = "dsn=oracle_test"
  --let connectionString = "dsn=db2_test"
  conn <- H.connectODBC connectionString
  putStrLn "\nbefore create\n"
  stmt1 <- H.prepare conn "create table fred (nm varchar(100) not null)"
  a1 <- H.execute stmt1 []
  print a1

  putStrLn "\nbefore insert\n"
  stmt2 <- H.prepare conn "insert into fred values(?)"
  a2 <- H.execute stmt2 [H.SqlString "hello"]
  print a2

  putStrLn "\nbefore select\n"
  stmt3 <- H.prepare conn "select nm,length(nm) from fred"
  vals3 <- H.execute stmt3 []
  print vals3
  results3 <- H.fetchAllRowsAL' stmt3
  putStrLn "select after insert"
  print results3

  putStrLn "\nbefore update\n"
  stmt4 <- H.prepare conn "update fred set nm=?"
  a4 <- H.execute stmt4 [H.SqlString "worldly"]
  print a4

  putStrLn "\nbefore select #2\n"
  stmt5 <- H.prepare conn "select nm,length(nm) from fred"
  vals5 <- H.execute stmt5 []
  print vals5
  results <- H.fetchAllRowsAL' stmt5
  putStrLn "select after update"
  print results

  H.commit conn

