{-
 - Example / basic pcap queries
 -}

module BasicQueries where

import Data.Function ((&))
import Data.Word
import Data.Bits

import qualified StreamMP as S
import qualified Packets as P
import Common

-- 
-- Query to copmute total number of packets and bytes in each epoch
--
totalPacketBytes :: PacketQuery
totalPacketBytes n s =
    s
    & S.map (\p -> (P.timeS p, (1 :: Word32, P.ipv4_len p & fromIntegral :: Word32)))
    & S.keyBy n (\_ -> 0 :: Int) id
    & S.windowByKeyed (floor . (* 1) . fst) snd
    & S.foldKeyedWindowed (\(p, b) (p', b') -> (p + p', b + b')) (0, 0)
    & S.map (\(t, (_, (p, b))) -> [show t, show p, show b])
    & S.printCSV

--
-- Query to compute packets and bytes from each source in each epoch
-- 
sources :: PacketQuery
sources n s =
    s
    & S.map (\p -> (P.ipv4_src p, (P.timeS p, (1 :: Word32, P.ipv4_len p & fromIntegral :: Word32))))
    & S.keyBy n fst snd
    & S.windowByKeyed (floor . (* 1) . fst) snd
    & S.foldKeyedWindowed (\(p, b) (p', b') -> (p + p', b + b')) (0, 0)
    & S.map (\(t, (s, (p, b))) -> [show t, show t, P.ipv4_to_string s, show p, show b])
    & S.printCSV

-- 
-- Query to produce list of distinct sources seen in entire trace
-- 
distinctSources :: PacketQuery
distinctSources n s =
    s
    & S.keyBy n P.ipv4_src id
    & S.distinctKeyed
    & S.mapM (putStrLn . P.ipv4_to_string)

-- 
-- Query to produce list of distinct TCP sources seen in entire trace
-- 
distinctTCPSources :: PacketQuery
distinctTCPSources n s =
    s
    & S.filter P.has_tcp
    & S.keyBy n P.ipv4_src id
    & S.distinctKeyed
    & S.mapM (putStrLn . P.ipv4_to_string)

-- 
-- Query to produce list of distinct sources along with some other info for filtering later
-- 
distinctSourcesInfo :: PacketQuery
distinctSourcesInfo n s =
    s
    & S.keyBy n key id
    & S.foldKeyed firstTime Nothing
    & S.map (\((src, proto, sport), Just t) -> [show t, P.ipv4_to_string src, show proto, show sport])
    & S.printCSV
    where key p =
            let sport = if P.has_udp p
                        then P.udp_sport p
                        else 0
            in (P.ipv4_src p, P.ipv4_proto p, sport)

          firstTime Nothing p = Just (P.timeS p)
          firstTime (Just t) _ = Just t

-- 
-- Query to produce total number of packets and bytes from each source over entire trace
--
sourcesAllTrace :: PacketQuery
sourcesAllTrace n s =
    s
    & S.map (\p -> (P.ipv4_src p, (1 :: Word32, P.ipv4_len p & fromIntegral :: Word32)))
    & S.keyBy n fst snd
    & S.foldKeyed (\(p, b) (p', b') -> (p + p', b + b')) (0, 0)
    & S.map (\(s, (p, b)) -> [P.ipv4_to_string s, show p, show b])
    & S.printCSV
