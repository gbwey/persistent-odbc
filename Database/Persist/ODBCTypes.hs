{-# LANGUAGE OverloadedStrings #-}
--{-# LANGUAGE TypeFamilies #-}
--{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE Rank2Types #-}
module Database.Persist.ODBCTypes where
import Data.Text (Text)
import Database.Persist.Sql

-- | List of DBMS that are supported
data DBType = MySQL | Postgres | MSSQL { mssql2012::Bool} | Oracle { oracle12c::Bool } deriving (Show,Read)

--load up the dbmsMigration depending on the dbms then somehow pass in the partially apply if it is limitoffset-capable to dbmsLimitOffset

data MigrationStrategy = MigrationStrategy { 
                            dbmsLimitOffset :: (Int,Int) -> Bool -> Text -> Text 
                           ,dbmsMigrate :: Show a => [EntityDef a] -> (Text -> IO Statement) -> EntityDef SqlType -> IO (Either [Text] [(Bool, Text)])
                           ,dbmsInsertSql :: DBName -> [DBName] -> DBName -> InsertSqlResult
                           ,dbmsEscape :: DBName -> Text
                           ,dbmsType :: DBType
                           }

{-
-- mysql: export this
dbmsMigration = MigrationStrategy { dbmsLimitOffset=decorateSQLWithLimitOffset "LIMIT 18446744073709551615" 
                           ,dbmsMigrate=migrateMysql
                           ,dbmsInsertSql=insertSqlMysql
                           ,dbmsEscape=escape 
                          }
-- mssql: export this
dbmsMigration = MigrationStrategy 
                          { dbmsLimitOffset=limitOffset 
                           ,dbmsMigrate=migrateMssql
                           ,dbmsInsertSql=insertSqlMssql
                           ,dbmsEscape=escape 
                          }
-- oracle: export this
dbmsMigration = MigrationStrategy { dbmsLimitOffset=limitOffset 
                           ,dbmsMigrate=migrateOracle
                           ,dbmsInsertSql=insertSqlOracle
                           ,dbmsEscape=escape 
                          }


limitOffset::DBType -> (Int,Int) -> Bool -> Text -> Text 
limitOffset dbtype (limit,offset) hasorder sql = trace ("dbtype=" ++ show dbtype ++ " limitoffset=" ++ show (limit,offset) ++ " hasorder=" ++ show hasorder ++ " sql=" ++ show sql) $
  case dbtype of
    Postgres -> decorateSQLWithLimitOffset "LIMIT ALL" (limit,offset) hasorder sql 
    MySQL -> decorateSQLWithLimitOffset "LIMIT 18446744073709551615" (limit,offset) hasorder sql 
    MSSQL { mssql2012=flag } -> MSSQL.limitOffset flag (limit,offset) hasorder sql 
    Oracle { oracle12c=flag } -> ORACLE.limitOffset flag (limit,offset) hasorder sql 
migrate' :: Show a => DBType 
         -> [EntityDef a]
         -> (Text -> IO Statement)
         => EntityDef SqlType
         -> IO (Either [Text] [(Bool, Text)])
migrate' dbt allDefs getter val = 
  case dbt of
    Postgres -> PG.migratePostgres allDefs getter val
    MySQL -> MYSQL.migrateMySQL allDefs getter val
    MSSQL {} -> MSSQL.migrateMSSQL allDefs getter val
    Oracle {} -> ORACLE.migrateOracle allDefs getter val
    -- _ -> error $ "no handler for " ++ show dbt

-- need different escape strategies for each type
-- need different ways to get the id back:returning works for postgres
-- SELECT LAST_INSERT_ID(); for mysql

insertSql' :: DBType -> DBName -> [DBName] -> DBName -> InsertSqlResult
insertSql' dbtype t cols id' = 
  case dbtype of
    Postgres -> PG.insertSqlPostgres t cols id'
    MySQL -> MYSQL.insertSqlMySQL t cols id'
    MSSQL {} -> MSSQL.insertSqlMSSQL t cols id'
    Oracle {} -> ORACLE.insertSqlOracle t cols id'
--    _ -> error $ "no handler for " ++ show dbtype

escape :: DBType -> DBName -> Text
escape dbtype dbname = 
  case dbtype of
    Postgres -> PG.escape dbname
    MySQL -> T.pack $ MYSQL.escapeDBName dbname
    MSSQL {} -> T.pack $ MSSQL.escapeDBName dbname
    Oracle {} -> T.pack $ ORACLE.escapeDBName dbname
    -- _ -> error $ "no escape handler for " ++ show dbtype

-}