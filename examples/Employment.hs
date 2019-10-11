-- @Employment.hs
{-# OPTIONS -Wall #-}
{-# LANGUAGE TemplateHaskell #-}
module Employment where

import Database.Persist.TH

data Employment = Employed | Unemployed | Retired
    deriving (Show, Read, Eq)
derivePersistField "Employment"
