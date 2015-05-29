{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
module Main where

import Control.Lens hiding (argument)
import Control.Monad
import Control.Monad.IO.Class
import Data.Aeson.Lens
import qualified Data.ByteString.Char8 as C8
import Data.Monoid
import qualified Data.Text as T
import Data.Text.Encoding (encodeUtf8)
import qualified Network.Socket as N
import Options.Applicative
import Statsd
import System.IO
import System.ZMQ3.Monadic

data Params = Params {
    statsdHost :: String
  , statsdPort :: String
  , zeromqConn :: String
  } deriving (Eq, Show)

parseArgs :: Parser Params
parseArgs = Params
  <$> argument str (metavar "STATSD_HOST")
  <*> argument str (metavar "STATSD_PORT")
  <*> argument str (metavar "ZEROMQ_CONNECTION_PATH")

processMessage :: N.Family
               -> N.SocketType
               -> N.ProtocolNumber
               -> N.SockAddr
               -> C8.ByteString
               -> IO ()
processMessage fam sckType proto addr msg =
  case msg ^? key "topic" . _String of
    Just s' -> do
      putStrLn $ "Increasing " ++ T.unpack s'
      incCounter s'
    Nothing -> return () --putStrLn "Ouch! There was no 'topic' key!"
  where
    incCounter str' = runStatsd fam sckType proto addr $
      statsdCounter ("fedmsg." <> encodeUtf8 str') 1

main' :: Params -> IO ()
main' (Params host port conn)  = do
  let hints   = N.defaultHints
                  { N.addrFamily     = N.AF_INET
                  , N.addrSocketType = N.Datagram
                  }

  N.AddrInfo{..}:_ <- N.getAddrInfo (Just hints) (Just host) (Just port)

  runZMQ $ do
    sub <- socket Sub
    subscribe sub ""
    connect sub conn
    forever $ do
      receive sub >>=
        liftIO . processMessage
                   addrFamily
                   addrSocketType
                   addrProtocol
                   addrAddress
      liftIO $ hFlush stdout

main :: IO ()
main = execParser opts >>= main'
  where
    opts = info (helper <*> parseArgs)
      ( fullDesc
     <> progDesc "Proxy fedmsg message topics into statsd for metrics keeping"
     <> header "fedmsg2statsd - A fedmsg -> statsd proxy" )
