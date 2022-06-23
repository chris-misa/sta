{- 
 - Main dispatch and driver
 -}
module Main (main) where

import Data.Function ((&))
import System.Environment
import Data.Word
import Data.Bits
import Data.List (intercalate)

import qualified StreamMP as S
import qualified Packets as P
import Common

import qualified BasicQueries as BQ

--
-- Link in new queries here by providing a string query identifier
-- For ease of reading try to make query identifier match the corresponding function name
--
queryList :: [(String, PacketQuery)]
queryList = [
        ("totalPacketBytes",         BQ.totalPacketBytes),
        ("sources",                  BQ.sources),
        ("distinctSources",          BQ.distinctSources),
        ("distinctTCPSources",       BQ.distinctTCPSources),
        ("distinctSourcesInfo",      BQ.distinctSourcesInfo),
        ("sourcesAllTrace",          BQ.sourcesAllTrace),
        ("distinctSrcsDstsPerEpoch", BQ.distinctSrcsDstsPerEpoch)
    ]

processPcapFile :: String -> Int -> PacketQuery -> IO ()
processPcapFile filepath n query = do
    pkts <- P.readPcapFile filepath
    S.repeatWhileJustM pkts & query n & S.runStream
    return ()

usage :: String
usage = "Required arguments: <degree of parallelism> <query identifier> <filepath>\n\n"
    ++ "Current query identifiers:\n"
    ++ (fmap fst queryList & intercalate ", ") ++ "\n"

main :: IO ()
main = do
    args <- getArgs
    case args of
        [n, queryId, filepath] ->
            let theQuery = lookup queryId queryList
            in case theQuery of
                Just q -> processPcapFile filepath (read n) q
                Nothing -> do
                    putStrLn $ "Unknown query identifier: \"" ++ queryId ++ "\""
                    putStrLn usage
        _ -> putStrLn usage
