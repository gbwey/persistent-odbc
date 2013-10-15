{-# LANGUAGE QuasiQuotes, TemplateHaskell, TypeFamilies, OverloadedStrings #-}
{-# LANGUAGE GADTs, FlexibleContexts #-}
{-# LANGUAGE EmptyDataDecls    #-}
{-# LANGUAGE FlexibleInstances, MultiParamTypeClasses #-}
module Test1 where

import qualified Database.Persist as P
import Database.Persist.TH
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Logger
import Data.Text (Text)
import qualified Data.Text as T
import Database.Persist.ODBC
import Data.Conduit
import Data.Aeson
import System.Environment (getArgs)

import qualified Database.Esqueleto as E
import Database.Esqueleto (select,where_,(^.),from,Value(..))
import Control.Applicative ((<$>),(<*>))
import Data.Int
import Debug.Trace
import Control.Monad (when)

share [mkPersist sqlOnlySettings, mkMigrate "migrateAll"] [persistLowerCase|
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
  UniqueSomeName name

Person 
  name String
  age Int Maybe
  deriving Eq Show
Address
  address String
  country String
  deriving Eq Show
PersonAddressComp
  person PersonId
  address AddressId
  Primary person address
  deriving Eq Show
PersonAddressNon
  person PersonId
  address AddressId
  deriving Eq Show

PersonAddressTest
  person PersonId
  address AddressId
  name String
  age Int Maybe
  Primary person address
  deriving Eq Show
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
           "q" -> (Sqlite,"dsn=sqlite_test")
           xs -> error $ "unknown option:choose p m s so o on d q found[" ++ xs ++ "]"

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
    zzz <- get m3
    liftIO $ putStrLn $ "after get again : should have failed zzz=" ++ show zzz
    
    bb <- selectList ([]::[Filter TableManyMany]) []
    liftIO $ putStrLn $ "bb=" ++ show bb ++"\n\n"

    xs <- select $ 
             from $ \ln -> do
                where_ (ln ^. TableManyRefone E.<=. E.val a1)
                E.orderBy [E.asc (ln ^. TableManyRefone)]
--                E.limit 3
--                E.offset 2
                return ln
    liftIO $ putStrLn $ show (length xs) ++ " rows: limit=3 offset=2 xs=" ++ show xs

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
  deleteWhere ([]::[Filter PersonAddressNon])
  deleteWhere ([]::[Filter PersonAddressComp])
  deleteWhere ([]::[Filter PersonAddressTest])
  deleteWhere ([]::[Filter Person])
  deleteWhere ([]::[Filter Address])
  let p@(Entity pk pv) = Entity undefined $ Person "Abraham" (Just 33)
  let a@(Entity ak av) = Entity undefined $ Address "12 Abel Smith St" "NZ" 
  let comp@(Entity compk compv) = Entity undefined $ PersonAddressComp pk ak
  let non@(Entity nonk nonv) = Entity undefined $ PersonAddressNon pk ak
  
  p1 <- insert $ Person "Abraham" (Just 33)
  a1 <- insert $ Address "12 Abel Smith St" "NZ" 

  abc <- insert $ PersonAddressTest p1 a1 "dude" $ Just 888
  liftIO $ print abc

  comp1 <- insert $ PersonAddressComp p1 a1
  non1 <- insert $ PersonAddressNon p1 a1
  liftIO $ print (p1,a1,comp1,non1)
  let zz = PersistList [unKey p1, unKey a1]
  liftIO $ print (zz, unKey comp1)
  liftIO $ print $ unKey comp1 == zz
  liftIO $ print (getInt64 (unKey non1))

  p2 <- insert $ Person "Fred" Nothing
  a2 <- insert $ Address "123 Baker Rd" "Aus" 
  comp1 <- insert $ PersonAddressComp p2 a2
  non1 <- insert $ PersonAddressNon p2 a2
  
  cs <- selectList [] [Desc PersonAddressCompPerson]
  ns <- selectList [] [Desc PersonAddressNonPerson]
  liftIO $ putStrLn $ "cs=" ++ show cs
  liftIO $ putStrLn $ "ns=" ++ show ns

  liftIO $ putStrLn $ "cs=" ++ show (map matchCompKV cs)
  liftIO $ putStrLn $ "ns=" ++ show (map matchNonKV ns)
  
  liftIO $ putStrLn $ "cs==ns = " ++ show (map matchNonKV ns==map matchCompKV cs)
  
  [pa3,pa4] <- insertMany [ PersonAddressComp p1 a2
                           , PersonAddressComp p2 a1
                           ]
  cs <- selectList [PersonAddressCompPerson ==. p1, PersonAddressCompAddress ==. a1] [Desc PersonAddressCompPerson]
  ns <- selectList [PersonAddressNonPerson ==. p1, PersonAddressNonAddress ==. a1] [Desc PersonAddressNonPerson]
  liftIO $ putStrLn $ "cs==ns = " ++ show (map matchNonKV ns==map matchCompKV cs)
  
  _ <- delete pa3
  c <- count ([]::[Filter PersonAddressComp])
  liftIO $ print (c,c==3)
  one <- deleteWhereCount [PersonAddressCompPerson ==. p1, PersonAddressCompAddress ==. a1]
  liftIO $ print (one,one==1)
  
  c <- count ([]::[Filter PersonAddressComp])
  liftIO $ print (c,c==2)
  
  h4 <- get pa4
  liftIO $ print pa4
  --PersonAddressComp p2 a1
  let k4a = maybe (Left "") matchCompV h4
  let k4e = (,) <$> getInt64 (unKey p2) <*> getInt64 (unKey a1)
  liftIO $ print (k4a,k4e,k4a==k4e)
  
  update pa3 [PersonAddressCompPerson =. p2] -- pa3 no longer points to the same row cos we changed the key!
  --update pa
  return ()

matchCompKV (Entity k v) = matchCompV v
matchNonKV  (Entity k v) = matchNonV v

matchCompV :: PersonAddressComp -> Either Text (Int64, Int64)
matchCompV PersonAddressComp { personAddressCompPerson = Key k1, personAddressCompAddress = Key k2 } = (,) <$> fromPersistValue k1 <*> fromPersistValue k2

matchNonV :: PersonAddressNon -> Either Text (Int64, Int64)
matchNonV  PersonAddressNon  { personAddressNonPerson = Key k1, personAddressNonAddress = Key k2 } = (,) <$> fromPersistValue k1 <*> fromPersistValue k2

getInt64 :: PersistValue -> Either Text Int64
getInt64 (PersistInt64 a) = Right a
getInt64 tp = Left $ T.pack $ show tp
