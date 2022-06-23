{- 
 - Main dispatch and driver
 -}
module Main (main) where

import Data.Function ((&))
import System.Environment
import Data.Word
import Data.Bits

import qualified StreamMP as S
import qualified Packets as P
import Common

import qualified BasicQueries as BQ

--
-- Link in new queries here by providing a string query identifier
-- For ease of reading try to make query identifier match the corresponding function name
--
getQuery :: String -> PacketQuery
getQuery qid =
    case qid of
        "totalPacketBytes"    -> BQ.totalPacketBytes
        "sources"             -> BQ.sources
        "distinctSources"     -> BQ.distinctSources
        "distinctTCPSources"  -> BQ.distinctTCPSources
        "distinctSourcesInfo" -> BQ.distinctSourcesInfo
        "sourcesAllTrace"     -> BQ.sourcesAllTrace

processPcapFile :: String -> Int -> PacketQuery -> IO ()
processPcapFile filepath n query = do
    pkts <- P.readPcapFile filepath
    S.repeatWhileJustM pkts & query n & S.runStream
    return ()

usage :: String
usage = "<degree of parallelism> <query identifier> <filepath>"

main :: IO ()
main = do
    args <- getArgs
    case args of
        [n, queryId, filepath] ->
            let theQuery = getQuery queryId
            in processPcapFile filepath (read n) theQuery
        _ -> putStrLn usage
