{-# LANGUAGE QuasiQuotes, TemplateHaskell, TypeFamilies, OverloadedStrings #-}
{-# LANGUAGE GADTs, FlexibleContexts #-}
{-# LANGUAGE EmptyDataDecls    #-}
module TestODBC where

import Database.Persist
import Database.Persist.ODBC
import Database.Persist.TH
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Resource (runResourceT)
import Control.Monad.Logger
import Database.HDBC 
import Database.HDBC.ODBC
import Text.Shakespeare.Text
import qualified Data.Text.Lazy 
import Data.Time (getCurrentTime,UTCTime)
import System.Environment (getArgs)
import Employment
import Data.Conduit
import qualified Data.Conduit.List as CL

import qualified Database.HDBC as H
import qualified Database.HDBC.ODBC as H
share [mkPersist sqlSettings, mkMigrate "migrateAll"] [persistLowerCase|
Test0 
    mybool Bool
    deriving Show
Test1 
    flag Bool
    flag1 Bool Maybe
    dbl Double 
    db2 Double Maybe
    deriving Show
    
Persony
    name String
    employment Employment
    deriving Show
    
Personx
    name String Eq Ne Desc
    age Int Lt Asc
    color String Maybe Eq Ne
    PersonxNameKey name
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
Person
    name String
    age Int Maybe
    deriving Show
BlogPost
    title String
    authorId PersonId
    deriving Show
|]

main :: IO ()
main = do
  [arg] <- getArgs
  let (dbtype,dsn) = 
       case arg of
           "p" -> (Postgres,"dsn=pg_gbtest")
           "m" -> (MySQL,"dsn=mysql_test")
           "s" -> (MSSQL,"dsn=mssql_testdb; Trusted_Connection=True")
           "o" -> (Oracle,"dsn=oracle_testdb;")
           xs -> error $ "unknown option:choose p m s o found[" ++ xs ++ "]"

  runResourceT $ runNoLoggingT $ withODBCConn dbtype dsn $ runSqlConn $ do
    liftIO $ putStrLn $ "\nbefore migration\n"
    runMigration migrateAll
    liftIO $ putStrLn $ "after migration"
    a1 <- insert $ Foo "test"
    liftIO $ putStrLn $ "a1=" ++ show a1
    aa <- selectList ([]::[Filter Foo]) []
    liftIO $ putStrLn $ "aa=" ++ show aa
    johnId <- insert $ Person "John Doe" $ Just 35
    liftIO $ putStrLn $ "johnId[" ++ show johnId ++ "]"
    janeId <- insert $ Person "Jane Doe" Nothing
    liftIO $ putStrLn $ "janeId[" ++ show janeId ++ "]"

    _ <- insert $ BlogPost "My fr1st p0st" johnId
    _ <- insert $ BlogPost "One more for good measure" johnId

    --oneJohnPost <- selectList [BlogPostAuthorId ==. johnId] [LimitTo 1]
    --liftIO $ print (oneJohnPost :: [Entity BlogPost])

    john <- get johnId
    liftIO $ print (john :: Maybe Person)
    dt <- liftIO getCurrentTime
    v1 <- insert $ Testnum 100 Nothing "hello" (Just "world") dt Nothing
    v2 <- insert $ Testnum 100 (Just 222) "dude" Nothing dt (Just "something")
    liftIO $ putStrLn $ "v1=" ++ show v1
    liftIO $ putStrLn $ "v2=" ++ show v2

    delete janeId
    deleteWhere [BlogPostAuthorId ==. johnId]
    test3
    test0 dbtype
    testa 
    if True then testb else return ()

testa = do
    aaa <- insert $ Test0 False 
    liftIO $ putStrLn $ show aaa
        
testb = do    
    a1 <- insert $ Test1 True (Just False) 100.3 Nothing
    liftIO $ putStrLn $ "a1=" ++ show a1
    a2 <- insert $ Test1 False Nothing 100.3 (Just 12.44)
    liftIO $ putStrLn $ "a2=" ++ show a2
    a3 <- insert $ Test1 True (Just True) 100.3 (Just 11.11)
    liftIO $ putStrLn $ "a3=" ++ show a3
    a4 <- insert $ Test1 False Nothing 100.3 Nothing
    liftIO $ putStrLn $ "a4=" ++ show a4
    ret <- selectList ([]::[Filter Test1]) [] 
    liftIO $ putStrLn $ "ret=" ++ show ret

    
test0 dbtype = do
    pid <- insert $ Persony "Dude" Retired
    liftIO $ print pid
    pid <- insert $ Persony "Dude1" Employed
    liftIO $ print pid
    pid <- insert $ Persony "Snoyman aa" Unemployed
    liftIO $ print pid
    pid <- insert $ Persony "bbb Snoyman" Employed
    liftIO $ print pid

    let sql = case dbtype of 
                Oracle -> "SELECT \"name\" FROM \"persony\" WHERE \"name\" LIKE '%Snoyman%'"
                _  -> "SELECT name FROM persony WHERE name LIKE '%Snoyman%'"
    rawQuery sql [] $$ CL.mapM_ (liftIO . print)
    
    
test3 = do
    --initialize (undefined :: Personx)
    pid <- insert $ Personx "Michael" 25 Nothing
    liftIO $ print pid

    p1 <- get pid
    liftIO $ putStrLn $ show p1

    replace pid $ Personx "Michael" 26 Nothing
    p2 <- get pid
    liftIO $ print p2

    p3 <- selectList [PersonxName ==. "Michael"] []
    liftIO $ print p3

    _ <- insert $ Personx "Michael2" 27 Nothing
    deleteWhere [PersonxName ==. "Michael2"]
    p4 <- selectList [PersonxAge <. 28] []
    liftIO $ print p4

    update pid [PersonxAge =. 28]
    p5 <- get pid
    liftIO $ print p5

    updateWhere [PersonxName ==. "Michael"] [PersonxAge =. 29]
    p6 <- get pid
    liftIO $ print p6

    _ <- insert $ Personx "Eliezer" 2 $ Just "blue"
    p7 <- selectList [] [Asc PersonxAge]
    liftIO $ print p7

    _ <- insert $ Personx "Abe" 30 $ Just "black"
    p8 <- selectList [PersonxAge <. 30] [Desc PersonxName]
    liftIO $ print p8

    _ <- insert $ Personx "Abe1" 31 $ Just "brown"
    p9 <- selectList [PersonxName ==. "Abe1"] []
    liftIO $ print p9

    p10 <- getBy $ PersonxNameKey "Michael"
    liftIO $ print p10

    p11 <- selectList [PersonxColor ==. Just "blue"] []
    liftIO $ print p11

    p12 <- selectList [PersonxColor ==. Nothing] []
    liftIO $ print p12

    p13 <- selectList [PersonxColor !=. Nothing] []
    liftIO $ print p13

    delete pid
    plast <- get pid
    liftIO $ print plast

    