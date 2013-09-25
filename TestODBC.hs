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
import Data.Text.Lazy 
import Data.Time (UTCTime)

-- import qualified Data.Text.Lazy.IO as TLIO
-- http://www.yesodweb.com/book/shakespearean-templates
-- TLIO.putStrLn [lt|You have #{show $ itemQty item} #{itemName item}.|]

share [mkPersist sqlSettings, mkMigrate "migrateAll"] [persistLowerCase|
Testnum
    bar Int
    znork Int Maybe
    znork1 String 
    znork2 String Maybe
    znork3 UTCTime
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
-- main = runResourceT $ runNoLoggingT $ withODBCConn (Just Postgres) "dsn=pg_gbtest" $ runSqlConn $ do
main = runResourceT $ runNoLoggingT $ withODBCConn (Just MySQL) "dsn=mysql_test" $ runSqlConn $ do
    runMigration migrateAll
    _ <- insert $ Foo "test"
    liftIO $ putStrLn $ "yes!!!!"
    johnId <- insert $ Person "John Doe" $ Just 35
    liftIO $ putStrLn $ "johnId[" ++ show johnId ++ "]"
    janeId <- insert $ Person "Jane Doe" Nothing
    liftIO $ putStrLn $ "janeId[" ++ show janeId ++ "]"

    _ <- insert $ BlogPost "My fr1st p0st" johnId
    _ <- insert $ BlogPost "One more for good measure" johnId

    oneJohnPost <- selectList [BlogPostAuthorId ==. johnId] [LimitTo 1]
    liftIO $ print (oneJohnPost :: [Entity BlogPost])

    john <- get johnId
    liftIO $ print (john :: Maybe Person)

    --delete janeId
    --deleteWhere [BlogPostAuthorId ==. johnId]
-- handles dml + multiple statements at once    
main1::IO ()
main1 = handleSqlError $ do 
  conn <- connectODBC "dsn=pg_gbtest"
  a1 <- runRaw conn "drop table foo; drop sequence foo_id_seq;" 
  print a1
  commit conn 
  let t3=[lt|
    CREATE sequence foo_id_seq START WITH 1 increment BY 1 no maxvalue no minvalue cache 20 no cycle;
    CREATE TABLE foo (
    id integer default nextval ('"foo_id_seq"'::regclass) not null,
    PRIMARY KEY (id),
    name character varying not null
    );
  |]
  a3 <- runRaw conn $ Data.Text.Lazy.unpack t3
  print a3
  commit conn 
  disconnect conn
  putStrLn "done"

    