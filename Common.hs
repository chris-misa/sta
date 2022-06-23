module Common where

import qualified StreamMP as S
import qualified Packets as P

--
-- General type for packet-level queries parameterized by
-- the degree of parallelism.
--
type PacketQuery = Int -> S.Stream P.Packet -> S.Stream ()
