{-# LANGUAGE TemplateHaskell #-}
-- this creates a field type so you can use it as a type on a entity column 
-- not so useful for ftype
module FtypeEnum where

import Database.Persist.TH
import qualified Prelude as P

data FtypeEnum = 
    Xsd_string
  | Xsd_boolean
  | Xsd_decimal
  | Xsd_float
  | Xsd_double
  | Xsd_duration
  | Xsd_dateTime
  | Xsd_time
  | Xsd_date
  | Xsd_hexBinary
  | Xsd_base64Binary
  | Xsd_anyURI
  | Xsd_QName

-- derived types

  | Xsd_integer
  | Xsd_int
  | Xsd_short
  | Xsd_byte
  | Xsd_long
    deriving (P.Read, P.Eq, P.Show)

derivePersistField "FtypeEnum"

{-
instance Show FtypeEnum where
  show Xsd_string = "string"
  show Xsd_boolean = "boolean"
  show Xsd_decimal = "decimal"
  show Xsd_float = "float"
  show Xsd_double = "double"
  show Xsd_duration = "duration"
  show Xsd_dateTime = "dateTime"
  show Xsd_time = "time"
  show Xsd_date = "date"
  show Xsd_hexBinary = "hexBinary"
  show Xsd_base64Binary = "base64Binary"
  show Xsd_anyURI = "anyURI"
  show Xsd_QName = "QName"

-}

{-
gYearMonth
gYear
gMonthDay
gDay
gMonth
NOTATION
-}


