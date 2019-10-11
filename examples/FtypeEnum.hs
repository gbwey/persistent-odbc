{-# OPTIONS -Wall #-}
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


