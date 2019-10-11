{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE Rank2Types #-}
module Database.Persist.ODBCTypes where
import Data.Text (Text)
import Database.Persist.Sql

-- | List of DBMS that are supported
data DBType = MySQL | Postgres | MSSQL { mssql2012 :: Bool} | Oracle { oracle12c :: Bool } | DB2 | Sqlite { sqlite3619 :: Bool } deriving (Show,Read)

mysql,postgres,mssqlMin2012,mssql,oracleMin12c,oracle,db2,sqlite,sqliteMin3619 :: DBType
mysql = MySQL
postgres = Postgres
mssqlMin2012 = MSSQL True
mssql = MSSQL False
oracleMin12c = Oracle True
oracle = Oracle False
db2 = DB2
sqlite = Sqlite False
sqliteMin3619 = Sqlite True
--load up the dbmsMigration depending on the dbms then somehow pass in the partially apply if it is limitoffset-capable to dbmsLimitOffset

data MigrationStrategy = MigrationStrategy {
                            dbmsLimitOffset :: (Int,Int) -> Bool -> Text -> Text
                           ,dbmsMigrate :: [EntityDef] -> (Text -> IO Statement) -> EntityDef -> IO (Either [Text] [(Bool, Text)])
                           ,dbmsInsertSql :: EntityDef -> [PersistValue] -> InsertSqlResult
                           ,dbmsEscape :: DBName -> Text
                           ,dbmsType :: DBType
                           }
