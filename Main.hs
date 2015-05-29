{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
module Main where

import Control.Lens
import Control.Monad
import Control.Monad.IO.Class
import Data.Aeson.Lens
import qualified Data.ByteString.Char8 as C8
import Data.Monoid
import Data.Text as T
import Data.Text.Encoding (encodeUtf8)
import qualified Network.Socket as N
import Statsd
import System.IO
import System.ZMQ3.Monadic

processMessage :: N.Family
               -> N.SocketType
               -> N.ProtocolNumber
               -> N.SockAddr
               -> C8.ByteString
               -> IO ()
processMessage fam sckType proto addr str =
  case str ^? key "topic" . _String of
    Just s' -> do
      putStrLn $ "Increasing " ++ T.unpack s'
      incCounter s'
    Nothing -> return () --putStrLn "Ouch! There was no 'topic' key!"
  where
    incCounter str' = runStatsd fam sckType proto addr $
      statsdCounter ("fedmsg." <> encodeUtf8 str') 1

main :: IO ()
main = do
  let hints   = N.defaultHints
                  { N.addrFamily     = N.AF_INET
                  , N.addrSocketType = N.Datagram
                  }
      host    = "localhost"
      service = "8125"

  N.AddrInfo{..}:_ <- N.getAddrInfo (Just hints) (Just host) (Just service)

  runZMQ $ do
    sub <- socket Sub
    subscribe sub ""
    connect sub "tcp://hub.fedoraproject.org:9940"
    forever $ do
      receive sub >>=
        liftIO . processMessage
                   addrFamily
                   addrSocketType
                   addrProtocol
                   addrAddress
      liftIO $ hFlush stdout
