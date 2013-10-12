{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE Rank2Types #-}
module Database.Persist.ODBCTypes where
import Data.Text (Text)
import Database.Persist.Sql

-- | List of DBMS that are supported
data DBType = MySQL | Postgres | MSSQL { mssql2012::Bool} | Oracle { oracle12c::Bool } | DB2 | Sqlite deriving (Show,Read)

mysql,postgres,mssqlMin2012,mssql,oracleMin12c,oracle,db2,sqlite::DBType
mysql = MySQL
postgres = Postgres
mssqlMin2012 = MSSQL True
mssql = MSSQL False
oracleMin12c = Oracle True 
oracle = Oracle False
db2 = DB2
sqlite = Sqlite
--load up the dbmsMigration depending on the dbms then somehow pass in the partially apply if it is limitoffset-capable to dbmsLimitOffset

data MigrationStrategy = MigrationStrategy { 
                            dbmsLimitOffset :: (Int,Int) -> Bool -> Text -> Text 
                           ,dbmsMigrate :: Show a => [EntityDef a] -> (Text -> IO Statement) -> EntityDef SqlType -> IO (Either [Text] [(Bool, Text)])
                           ,dbmsInsertSql :: DBName -> [FieldDef SqlType] -> DBName -> [PersistValue] -> Bool -> InsertSqlResult
                           ,dbmsEscape :: DBName -> Text
                           ,dbmsType :: DBType
                           }
