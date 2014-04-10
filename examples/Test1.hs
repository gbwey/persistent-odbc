{-# LANGUAGE QuasiQuotes, TemplateHaskell, TypeFamilies, OverloadedStrings #-}
{-# LANGUAGE GADTs, FlexibleContexts #-}
{-# LANGUAGE EmptyDataDecls    #-}
{-# LANGUAGE FlexibleInstances, MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Test1 where

import qualified Database.Persist as P
import Database.Persist.TH
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Logger
import Control.Monad.Trans.Resource (runResourceT,MonadResource)
import Data.Text (Text)
import qualified Data.Text as T
import Database.Persist.ODBC
--import Data.Conduit
import Data.Aeson
import System.Environment (getArgs)

import qualified Database.Esqueleto as E
import Database.Esqueleto (select,where_,(^.),from,Value(..))
import Control.Applicative ((<$>),(<*>))
import Data.Int
import Debug.Trace
import Control.Monad (when)

share [mkPersist sqlOnlySettings, mkMigrate "migrateAll"] [persistLowerCase|
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
updatePersistValue (Update a1 v a2) = trace ("updatePersistValue a2="++show a2) $ toPersistValue v

main :: IO ()
main = do
  [arg] <- getArgs
  let (dbtype',dsn) = 
       case arg of -- odbc system dsn
           "d" -> (DB2,"dsn=db2_test")
           "p" -> (Postgres,"dsn=pg_test")
           "m" -> (MySQL,"dsn=mysql_test")
           "s" -> (MSSQL True,"dsn=mssql_test; Trusted_Connection=True") -- mssql 2012 [full limit and offset support]
           "so" -> (MSSQL False,"dsn=mssql_test; Trusted_Connection=True") -- mssql pre 2012 [limit support only]
           "o" -> (Oracle False,"dsn=oracle_test") -- pre oracle 12c [no support for limit and offset] 
           "on" -> (Oracle True,"dsn=oracle_test") -- >= oracle 12c [full limit and offset support]
           "q" -> (Sqlite False,"dsn=sqlite_test")
           "qn" -> (Sqlite True,"dsn=sqlite_test")
           xs -> error $ "unknown option:choose p m s so o on d q qn found[" ++ xs ++ "]"

  runResourceT $ runNoLoggingT $ withODBCConn Nothing dsn $ runSqlConn $ do
    conn <- askSqlConn
    let dbtype::DBType
        dbtype=read $ T.unpack $ connRDBMS conn
    liftIO $ putStrLn $ "original:" ++ show dbtype' ++ " calculated:" ++ show dbtype
    liftIO $ putStrLn "\nbefore migration\n"
    runMigration migrateAll
    liftIO $ putStrLn "after migration"
    
    doesq
    when True $ dostuff dbtype

dostuff dbtype = do    
    z1 <- insert $ TableOne "test1 aa"
    z2 <- insert $ TableOne "test1 bb"
    a1 <- insert $ TableOne "test1 cc"
    liftIO $ putStrLn $ "a1=" ++ show a1
    a2 <- insert $ TableTwo "test2"
    liftIO $ putStrLn $ "a2=" ++ show a2
    aa <- selectList ([]::[Filter TableOne]) []
    liftIO $ putStrLn $ "aa=" ++ show aa

    let b=encode $ head aa
    liftIO $ putStrLn $ "\ntojson for tableone " ++ show b
    
    let c=decode' b::Maybe (Entity TableOne)
    liftIO $ putStrLn $ "\nfromjson for tableone " ++ show c

    a3 <- insert $ TableMany a1 a2
    liftIO $ putStrLn $ "a3=" ++ show a3
    
    bb <- selectList ([]::[Filter TableMany]) []
    liftIO $ putStrLn $ "bb=" ++ show bb ++"\n\n" ++ show (toPersistValue z1)
    
    zz <- selectList [TableManyRefone <. a1] [Asc TableManyRefone]
    liftIO $ putStrLn $ "\n\n!!!TESTING FILTER zz=" ++ show zz ++"\n\n"
    
    let x=toJSON $ head bb
    liftIO $ putStrLn $ "\njson for a single tablemany " ++ show x

    let b=encode $ head bb
    liftIO $ putStrLn $ "\ntojson for a single tablemany " ++ show b
    
    let c=decode' b::Maybe (Entity TableMany)
    liftIO $ putStrLn $ "fromjson for tablemany " ++ show c

    let b1=encode bb
    liftIO $ putStrLn $ "\ntojson for a list of tablemany " ++ show b1
    
    let c1=decode' b1::Maybe [Entity TableMany]
    liftIO $ putStrLn $ "\nfromjson for list of tablemany " ++ show c1

    liftIO $ putStrLn $ "dude=" ++ show (updatePersistValue (TableManyRefone =. z1))

    p5 <- get a3
    liftIO $ putStrLn $ "after get!!!"
    liftIO $ print p5

    update a3 [TableManyRefone =. z1]

    p5 <- get a1
    liftIO $ putStrLn $ "after normal get!!!"
    liftIO $ print p5

    c1 <- insert $ TableOne "testa"
    c2 <- insert $ TableTwo "testb"
    c3 <- insert $ TableThree "testc"
    m3 <- insert $ TableManyMany c1 c2 c3
    liftIO $ putStrLn $ "m3=" ++ show m3

    liftIO $ putStrLn $ "before get zzz"
    zzz <- get m3
    liftIO $ putStrLn $ "before delete zzz=" ++ show zzz
    delete m3
    liftIO $ putStrLn $ "after delete m3"
    --zzz <- get m3
    --liftIO $ putStrLn $ "after get again : should have failed zzz=" ++ show zzz
    
    bb <- selectList ([]::[Filter TableManyMany]) []
    liftIO $ putStrLn $ "bb=" ++ show bb ++"\n\n"
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

    m3 <- insert $ TableManyMany c1 c2 c3

    a11 <- updateGet m3 [TableManyManyRefone =. c1] 
    liftIO $ putStrLn $ "a11=" ++ show a11
    case dbtype of 
      Oracle False -> liftIO $ putStrLn $ "oracle so no selectfirst"
      _ -> do
              a22 <- selectFirst [TableManyManyReftwo ==. c2] [Desc TableManyManyRefone]
              liftIO $ putStrLn $ "a22=" ++ show a22
    a33 <- count [TableManyManyReftwo <=. c2] 
    liftIO $ putStrLn $ "a33=" ++ show a33
    
    a33 <- count ([]::[Filter TableOne])
    liftIO $ putStrLn $ "before =" ++ show a33
    updateWhere [TableOneNameone ==. "freee"] [TableOneNameone =. "dude"]
    deleteWhere [TableOneNameone ==. "dude"] 
    a33 <- count ([]::[Filter TableOne])
    liftIO $ putStrLn $ "after =" ++ show a33

    p4 <- selectList [TableOneNameone >=. "a"] []
    liftIO $ print p4
    
    liftIO $ putStrLn $ "before selectKeys List 111"
    p4 <- selectKeysList [TableOneNameone >=. "a"] []
    liftIO $ print p4

    liftIO $ putStrLn $ "before selectKeys List 222"
    p4 <- selectKeysList [TableManyManyReftwo <=. c2] []
    liftIO $ print p4

--doesq::IO ()
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
