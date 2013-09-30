{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE Rank2Types #-}
module Database.Persist.ODBCTypes where
import Data.Text (Text)
import Database.Persist.Sql

-- | List of DBMS that are supported
data DBType = MySQL | Postgres | MSSQL { mssql2012::Bool} | Oracle { oracle12c::Bool } | DB2 deriving (Show,Read)

--load up the dbmsMigration depending on the dbms then somehow pass in the partially apply if it is limitoffset-capable to dbmsLimitOffset

data MigrationStrategy = MigrationStrategy { 
                            dbmsLimitOffset :: (Int,Int) -> Bool -> Text -> Text 
                           ,dbmsMigrate :: Show a => [EntityDef a] -> (Text -> IO Statement) -> EntityDef SqlType -> IO (Either [Text] [(Bool, Text)])
                           ,dbmsInsertSql :: DBName -> [FieldDef SqlType] -> DBName -> [PersistValue] -> InsertSqlResult
                           ,dbmsEscape :: DBName -> Text
                           ,dbmsType :: DBType
                           }
