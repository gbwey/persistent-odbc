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
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE DeriveGeneric #-}
module Test1 where
import qualified Database.Persist as P
import Database.Persist.TH
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Logger
import Control.Monad.Trans.Resource (runResourceT, ResourceT)
import Control.Monad.Trans.Reader (ask,ReaderT)
import qualified Data.Text as T
import Database.Persist.ODBC
import Data.Aeson
import System.Environment (getArgs)
import qualified Database.Esqueleto as E
import Database.Esqueleto (select,where_,(^.),from)
import Debug.Trace
import Control.Monad (when)

share [mkPersist sqlSettings, mkMigrate "migrateAll"] [persistLowerCase|
G0
    g1 String
    g2 String
    g3 String
    deriving Show Eq
G1
    g1 String maxlen=20
    g2 String
    g3 String
    Primary g1
    deriving Show Eq
G2
    g1 String maxlen=20
    g2 String maxlen=20
    g3 String
    Primary g1 g2
    deriving Show Eq
G3
    g1 G0Id
    g2 String
    g3 String maxlen=20
    Primary g1 g3   -- what does this mean? g1 is a foreign key
    deriving Show Eq
TestA
    name1 String
    name2 String
    Unique MyNames name1 name2
    deriving Show Eq
TestBool
    mybool Bool
    deriving Show Eq
TestTwoStrings
    firstname String
    lastname String
    deriving Show Eq
TableOne json
    nameone String
    deriving Show Eq
TableTwo json
    nametwo String
    deriving Show Eq
TableThree json
    namethree String
    deriving Show Eq
TableFour
    namefour Int
    deriving Show Eq
TableMany json
    refone TableOneId
    reftwo TableTwoId
    Primary refone reftwo
    deriving Show Eq
TableManyMany json
    refone TableOneId
    reftwo TableTwoId
    refthree TableThreeId
    Primary refone reftwo refthree
    deriving Show Eq
TableU
  refone TableOneId
  name String
  Unique SomeName name

|]

updatePersistValue :: Update v -> PersistValue
updatePersistValue (Update _ v a2) = trace ("updatePersistValue a2="++show a2) $ toPersistValue v
updatePersistValue _ = error "updatePersistValue: expected an Update but found"

main :: IO ()
main = do
  [arg] <- getArgs
  let (dbtype',dsn) =
       case arg of -- odbc system dsn
           "d" -> (DB2,"dsn=db2_test")
           "p" -> (Postgres,"dsn=pg_test")
           "m" -> (MySQL,"dsn=mysql_test")
           "s" -> (MSSQL True,"dsn=mssql_test") -- mssql 2012 [full limit and offset support]
           "so" -> (MSSQL False,"dsn=mssql_test") -- mssql pre 2012 [limit support only]
           "o" -> (Oracle False,"dsn=oracle_test") -- pre oracle 12c [no support for limit and offset]
           "on" -> (Oracle True,"dsn=oracle_test") -- >= oracle 12c [full limit and offset support]
           "q" -> (Sqlite False,"dsn=sqlite_test")
           "qn" -> (Sqlite True,"dsn=sqlite_test")
           xs -> error $ "unknown option:choose p m s so o on d q qn found[" ++ xs ++ "]"

  runResourceT $ runNoLoggingT $ withODBCConn Nothing dsn $ runSqlConn $ do
    conn <- ask
    let dbtype :: DBType
        dbtype=read $ T.unpack $ connRDBMS conn
    liftIO $ putStrLn $ "original:" ++ show dbtype' ++ " calculated:" ++ show dbtype
    liftIO $ putStrLn "\nbefore migration\n"
    runMigration migrateAll
    liftIO $ putStrLn "after migration"

    doesq
    when True $ testJson dbtype

testJson :: ( MonadIO m
           , BackendCompatible SqlBackend backend
           , PersistUniqueRead backend
           , PersistQueryWrite backend
           , BaseBackend backend ~ SqlBackend)
           => DBType
           -> ReaderT backend m ()
testJson dbtype = do
    z1 <- insert $ TableOne "test1 aa"
    liftIO $ print z1
    z2 <- insert $ TableOne "test1 bb"
    liftIO $ print z2
    a1 <- insert $ TableOne "test1 cc"
    liftIO $ putStrLn $ "a1=" ++ show a1
    a2 <- insert $ TableTwo "test2"
    liftIO $ putStrLn $ "a2=" ++ show a2
    aa <- selectList ([]::[Filter TableOne]) []
    liftIO $ putStrLn $ "aa=" ++ show aa

    let b1=encode $ head aa
    liftIO $ putStrLn $ "\ntojson for tableone " ++ show b1

    let c1=decode' b1 :: Maybe (Entity TableOne)
    liftIO $ putStrLn $ "\nfromjson for tableone " ++ show c1

    a3 <- insert $ TableMany a1 a2
    liftIO $ putStrLn $ "a3=" ++ show a3

    bb <- selectList ([]::[Filter TableMany]) []
    liftIO $ putStrLn $ "bb=" ++ show bb ++"\n\n" ++ show (toPersistValue z1)

    zz <- selectList [TableManyRefone <. a1] [Asc TableManyRefone]
    liftIO $ putStrLn $ "\n\n!!!TESTING FILTER zz=" ++ show zz ++"\n\n"

    let x=toJSON $ head bb
    liftIO $ putStrLn $ "\njson for a single tablemany " ++ show x

    let b2=encode $ head bb
    liftIO $ putStrLn $ "\ntojson for a single tablemany " ++ show b2

    let c2=decode' b2 :: Maybe (Entity TableMany)
    liftIO $ putStrLn $ "fromjson for tablemany " ++ show c2

    let b3=encode bb
    liftIO $ putStrLn $ "\ntojson for a list of tablemany " ++ show b3

    let c3=decode' b3 :: Maybe [Entity TableMany]
    liftIO $ putStrLn $ "\nfromjson for list of tablemany " ++ show c3

    liftIO $ putStrLn $ "dude=" ++ show (updatePersistValue (TableManyRefone =. z1))

    p5 <- get a3
    liftIO $ putStrLn $ "after get!!!"
    liftIO $ print p5

    update a3 [TableManyRefone =. z1]

    p6 <- get a1
    liftIO $ putStrLn $ "after normal get!!!"
    liftIO $ print p6

    c4 <- insert $ TableOne "testa"
    c5 <- insert $ TableTwo "testb"
    c6 <- insert $ TableThree "testc"
    m3 <- insert $ TableManyMany c4 c5 c6
    liftIO $ putStrLn $ "m3=" ++ show m3

    liftIO $ putStrLn $ "before get zzz"
    zzz <- get m3
    liftIO $ putStrLn $ "before delete zzz=" ++ show zzz
    delete m3
    liftIO $ putStrLn $ "after delete m3"
    --zzz <- get m3
    --liftIO $ putStrLn $ "after get again : should have failed zzz=" ++ show zzz

    bb1 <- selectList ([]::[Filter TableManyMany]) []
    liftIO $ putStrLn $ "bb1=" ++ show bb1 ++"\n\n"
{-
    xs <- select $
             from $ \ln -> do
                where_ (ln ^. TableManyRefone E.<=. E.val a1)
                E.orderBy [E.asc (ln ^. TableManyRefone)]
--                E.limit 3
--                E.offset 2
                return ln
    liftIO $ putStrLn $ show (length xs) ++ " rows: limit=3 offset=2 xs=" ++ show xs
-}
    xs <- select $
             from $ \ln -> do
                where_ (ln ^. TableOneNameone E.<=. E.val "test1 bb")
                E.orderBy [E.asc (ln ^. TableOneNameone)]
--                E.limit 4
--                E.offset 1
                return ln
    liftIO $ putStrLn $ show (length xs) ++ " rows: limit=2 offset=3 xs=" ++ show xs

    a11 <- updateGet a1 [TableOneNameone =. "freee"]
    liftIO $ putStrLn $ "a11=" ++ show a11
    case dbtype of
      Oracle False -> liftIO $ putStrLn $ "oracle so no selectfirst"
      _ -> do
              a22 <- selectFirst [TableOneNameone ==. "freee"] [Desc TableOneNameone]
              liftIO $ putStrLn $ "a22=" ++ show a22
    a33 <- count [TableOneNameone >=. "a"]
    liftIO $ putStrLn $ "a33=" ++ show a33

    m4 <- insert $ TableManyMany c4 c5 c6

    a44 <- updateGet m4 [TableManyManyRefone =. c4]
    liftIO $ putStrLn $ "a44=" ++ show a44
    case dbtype of
      Oracle False -> liftIO $ putStrLn $ "oracle so no selectfirst"
      _ -> do
              a22 <- selectFirst [TableManyManyReftwo ==. c5] [Desc TableManyManyRefone]
              liftIO $ putStrLn $ "a22=" ++ show a22
    a55 <- count [TableManyManyReftwo <=. c5]
    liftIO $ putStrLn $ "a55=" ++ show a55

    a66 <- count ([]::[Filter TableOne])
    liftIO $ putStrLn $ "before =" ++ show a66
    updateWhere [TableOneNameone ==. "freee"] [TableOneNameone =. "dude"]
    deleteWhere [TableOneNameone ==. "dude"]
    a77 <- count ([]::[Filter TableOne])
    liftIO $ putStrLn $ "after =" ++ show a77

    p7 <- selectList [TableOneNameone >=. "a"] []
    liftIO $ print p7

    liftIO $ putStrLn $ "before selectKeys List 111"
    p8 <- selectKeysList [TableOneNameone >=. "a"] []
    liftIO $ print p8

    liftIO $ putStrLn $ "before selectKeys List 222"
    p9 <- selectKeysList [TableManyManyReftwo <=. c5] []
    liftIO $ print p9

doesq :: ReaderT SqlBackend (NoLoggingT (ResourceT IO)) ()
doesq = do
  deleteWhere ([]::[Filter G3])
  deleteWhere ([]::[Filter G2])
  deleteWhere ([]::[Filter G1])
  deleteWhere ([]::[Filter G0])

  g1 <- insert $ G1 "aaa1" "bbb1" "ccc1"
  liftIO $ putStrLn $ "g1=" ++ show g1
  g2 <- insert $ G2 "aaa2" "bbb2" "ccc2"
  liftIO $ putStrLn $ "g2=" ++ show g2

  gg1 <- selectList [] [Desc G1G1]
  gg2 <- selectList [] [Desc G2G3]
  liftIO $ putStrLn $ "gg1=" ++ show gg1
  liftIO $ putStrLn $ "gg2=" ++ show gg2

  g0 <- insert $ G0 "aaa0" "bbb0" "ccc0"
  liftIO $ putStrLn $ "g1=" ++ show g1
  g3 <- insert $ G3 g0 "bbb3" "ccc3"
  liftIO $ putStrLn $ "g3=" ++ show g3
  gg3a <- selectList [] [Desc G3G1]
  liftIO $ putStrLn $ "gg3a=" ++ show gg3a
  gg3b <- selectList [] [Desc G3G3]
  liftIO $ putStrLn $ "gg3b=" ++ show gg3b
