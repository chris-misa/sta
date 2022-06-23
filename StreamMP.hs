{-# LANGUAGE BangPatterns #-}
module StreamMP where

import Prelude hiding (mapM, map, filter)
import Data.Function ((&))

import Control.Concurrent
import Control.Concurrent.MVar (newEmptyMVar, putMVar, readMVar, MVar)
import qualified Control.Monad as CM
import Control.Monad.Loops (unfoldM)

import Data.Hashable

import qualified Data.Vector as V
import Data.Vector ((!))
import qualified Data.List as L
import qualified Control.Concurrent.Chan.Unagi.Bounded as C
import qualified Data.HashTable.IO as H

import Control.DeepSeq (NFData, force)

type HashTable k v = H.BasicHashTable k v

data Message a = Data !a | Stop

type InChannel a = V.Vector (C.InChan (Message a))
type OutChannel a = V.Vector (C.OutChan (Message a))

data StreamData a = Stream {
    outs :: OutChannel a,
    procs :: [IO ()]
}

type Stream a = IO (StreamData a)

defaultDepth = 2^14

newChannel :: Int -> IO (InChannel a, OutChannel a)
newChannel n = do
    inouts <- V.sequence $ V.replicate n (C.newChan defaultDepth)
    return $ (V.map fst inouts, V.map snd inouts)

newStream :: OutChannel a -> [IO ()] -> Stream a
newStream outchans processes =
    return $ Stream {outs = outchans, procs = processes}

getStreamOP :: Stream a -> IO (OutChannel a, [IO ()])
getStreamOP stream = stream >>= (\Stream {outs = o, procs = p} -> return (o, p))

--
-- sources
--

fromList :: [a] -> Stream a
fromList l = do
    (dIns, dOuts) <- newChannel 1
    newStream dOuts [go (dIns ! 0) l]
    where go dstchan (x:xs) = do
            C.writeChan dstchan (Data x)
            go dstchan xs
          go dstchan [] = C.writeChan dstchan Stop


repeatWhileMapM :: (a -> Maybe b) -> IO a -> Stream b
repeatWhileMapM f action = do
    (dIns, dOuts) <- newChannel 1
    newStream dOuts [go (dIns ! 0) action]
    where go dstchan action = do
            xmaybe <- fmap f action
            case xmaybe of
                Just x -> do
                    C.writeChan dstchan (Data x)
                    go dstchan action
                Nothing -> C.writeChan dstchan Stop
    
repeatWhileJustM :: IO (Maybe a) -> Stream a
repeatWhileJustM = repeatWhileMapM id

fromFoldable :: Foldable t => t a -> Stream a
fromFoldable f = do
    (dIns, dOuts) <- newChannel 1
    newStream dOuts [go (dIns ! 0) f]
    where go dstchan f = foldMap (writeOne dstchan) f >> writeStop dstchan
          writeOne dstchan x = do
            C.writeChan dstchan (Data x)
          writeStop dstchan = C.writeChan dstchan Stop

-- 
-- one-to-one operators
--

-- in-degree = out-degree
-- proc is run starting from init for each parallel channel
make1to1Operator :: (s -> Message a -> IO (s, [Message b])) -> IO s -> Stream a -> Stream b
make1to1Operator proc init inStream = do
    (sOuts, sProcs) <- getStreamOP inStream
    let n = V.length sOuts
    (dIns, dOuts) <- newChannel n
    states <- V.sequence $ V.replicate n init
    newStream dOuts (sProcs ++ newProcs sOuts dIns states)
    where newProcs sOuts dIns states = V.zip3 sOuts dIns states & fmap go & V.toList
          go (srcChan, dstChan, state) = do
            x <- C.readChan srcChan
            (state', output) <- proc state x
            stopping <- CM.foldM sendOutput False output
            case stopping of
                True -> return ()
                False -> go (srcChan, dstChan, state')
            where sendOutput stopping (Data x) = C.writeChan dstChan (Data x) >> return stopping
                  sendOutput _ Stop = C.writeChan dstChan Stop >> return True

mapM :: (a -> IO b) -> Stream a -> Stream b
mapM f = make1to1Operator proc (return ())
    where proc () (Data x) = f x >>= (\y -> return ((), [Data y]))
          proc () Stop = return ((), [Stop])

map :: (a -> b) -> Stream a -> Stream b
map f = mapM (return . f)

flatMapM :: (a -> IO [b]) -> Stream a -> Stream b
flatMapM f = make1to1Operator proc (return ())
    where proc () (Data x) = f x >>= (\y -> return ((), fmap Data y))
          proc () Stop = return ((), [Stop])

flatMap :: (a -> [b]) -> Stream a -> Stream b
flatMap f = flatMapM (return . f)

filter :: (a -> Bool) -> Stream a -> Stream a
filter f = make1to1Operator proc (return ())
    where proc () (Data x) = if f x then return ((), [Data x]) else return ((), [])
          proc () Stop = return ((), [Stop])

filterM :: (a -> IO Bool) -> Stream a -> Stream a
filterM f = make1to1Operator proc (return ())
    where proc () (Data x) = do
            res <- f x
            if res then return ((), [Data x]) else return ((), [])
          proc () Stop = return ((), [Stop])

scan :: (b -> a -> b) -> b -> Stream a -> Stream b
scan f init = make1to1Operator proc (return init)
    where proc state (Data x) = let state' = f state x in return (state', [Data state'])
          proc state Stop = return (state, [Stop])

scan' :: (NFData b) => (b -> a -> b) -> b -> Stream a -> Stream b
scan' f init = make1to1Operator proc (return init)
    where proc state (Data x) = let state' = force (f state x) in return (state', [Data state'])
          proc state Stop = return (state, [Stop])

scanM :: (b -> a -> IO b) -> IO b -> Stream a -> Stream b
scanM f init = make1to1Operator proc init
    where proc state (Data x) = do
            state' <- f state x
            return (state', [Data state'])
          proc state Stop = return (state, [Stop])

-- 
-- Outputs state after each input element
--
scanKeyed :: (Hashable k, Eq k) => (b -> a -> b) -> b -> Stream (k, a) -> Stream (k, b)
scanKeyed f init = make1to1Operator proc initTable
    where proc table (Data (i, x)) = do
            res <- H.mutate table i doUpdate
            return (table, [Data (i, res)])
            where doUpdate Nothing = let res = f init x in (Just res, res)
                  doUpdate (Just s) = let res = f s x in (Just res, res)
          proc table Stop = do
            return (table, [Stop])
          initTable = H.new :: IO (HashTable a b)

fold :: (b -> a -> b) -> b -> Stream a -> Stream b
fold f init = make1to1Operator proc (return init)
    where proc state (Data x) = return (f state x, [])
          proc state Stop = return (state, [Data state, Stop])

foldM :: (b -> a -> IO b) -> IO b -> Stream a -> Stream b
foldM f init = make1to1Operator proc init
    where proc state (Data x) = do
            res <- f state x
            return (res, [])
          proc state Stop = return (state, [Data state, Stop])

-- 
-- Outputs state once when the stream terminates
--
foldKeyed :: (Hashable k, Eq k) => (b -> a -> b) -> b -> Stream (k, a) -> Stream (k, b)
foldKeyed f init = make1to1Operator proc initTable
    where proc table (Data (i, x)) = do
            H.mutate table i doUpdate
            return (table, [])
            where doUpdate Nothing = (Just $ f init x, ())
                  doUpdate (Just s) = (Just $ f s x, ())
          proc table Stop = do
            res <- H.foldM (\xs (i, x) -> return ((Data (i, x)):xs)) [Stop] table
            return (table, res)
          initTable = H.new :: IO (HashTable a b)

-- 
-- Outputs and resets state whenever epoch number of current element is different from epoch number of previous element
-- The input stream type assumes keyBy -> windowBy -> foldKeyedWindowed (as also indicated by the function's name)
--
foldKeyedWindowed :: (Hashable k, Eq k) => (b -> a -> b) -> b -> Stream (Int, (k, a)) -> Stream (Int, (k, b))
foldKeyedWindowed f init = make1to1Operator proc initState
    where proc (table, prevW) (Data (w, (i, x))) =
            if w == prevW
            then do
                H.mutate table i (doUpdate x)
                return ((table, prevW), [])
            else do
                res <- H.foldM (\xs (i', x') -> return ((Data (prevW, (i', x'))):xs)) [] table
                table' <- initTable
                H.mutate table' i (doUpdate x)
                return ((table', w), res)
            where doUpdate x' Nothing = (Just $ f init x', ())
                  doUpdate x' (Just s) = (Just $ f s x', ())
          proc (table, prevW) Stop = do
            res <- H.foldM (\xs (i, x) -> return ((Data (prevW, (i, x))):xs)) [Stop] table
            return ((table, prevW), res)
          initTable = H.new :: IO (HashTable a b)
          initState = initTable >>= (\t -> return (t, 0))

-- 
-- Assign epoch numbers to keyed elements
-- Note that epoch numbers start at zero and must be strictly non-decreasing
--
windowByKeyed :: (a -> Int) -> (a -> b) -> Stream (k, a) -> Stream (Int, (k, b))
windowByKeyed win value = map (\(i, a) -> (win a, (i, value a)))

distinctKeyed :: (Hashable k, Eq k) => Stream (k, a) -> Stream k
distinctKeyed = make1to1Operator proc initTable
    where proc table (Data (i, x)) = do
            res <- H.mutate table i doUpdate
            case res of
                True -> return (table, [Data i])
                False -> return (table, [])
            where doUpdate Nothing = (Just True, True)
                  doUpdate (Just x) = (Just x, False)
          proc table Stop = do
            return (table, [Stop])
          initTable = H.new :: IO (HashTable a b)

-- 
-- One-to-many operators
-- (may change degree)
--

-- in-degree is taken from inStream, out-degree = outDeg
-- proc is run for each channel in inStream and can send to any of the n out channels
-- proc must return Nothing when it's done and should not send any Stops
make1toManyOperator ::
       Int
    -> (s -> Message a -> InChannel b -> IO (Maybe s))
    -> (Int -> IO s)
    -> Stream a
    -> Stream b
make1toManyOperator outDeg proc init inStream = do
    (sOuts, sProcs) <- getStreamOP inStream
    (dIns, dOuts) <- newChannel outDeg
    let inDeg = V.length sOuts
    mvars <- V.sequence $ V.replicate inDeg newEmptyMVar
    states <- V.sequence $ ([0..] & take inDeg & fmap init & V.fromList)
    newStream dOuts (sProcs ++ newProcs sOuts dIns mvars states)
    where newProcs sOuts dIns mvars states =
            waitForStops dIns mvars : (V.zip3 states sOuts mvars & fmap (go dIns) & V.toList)
          go dIns (state, srcChan, mv) = do
            x <- C.readChan srcChan
            state' <- proc state x dIns
            case state' of
                Just s -> go dIns (s, srcChan, mv)
                Nothing -> putMVar mv ()
          waitForStops dIns mvars = do
            sequence $ fmap readMVar mvars
            sequence $ fmap (\ch -> C.writeChan ch Stop) dIns
            return ()

--
-- Redistribute to degree outDeg
--
scatter :: Int -> Stream a -> Stream a
scatter outDeg = make1toManyOperator outDeg proc init
    where proc i (Data x) dIns = do
            C.writeChan (dIns ! i) (Data x)
            let i' = (i + 1) `mod` V.length dIns
            return $ Just i'
          proc _ Stop _ = return Nothing
          init i = return $ i `mod` outDeg

--
-- Collect in a degree 1 output stream
--
gather :: Stream a -> Stream a
gather = make1toManyOperator 1 proc init
    where proc () (Data x) dIns = do
            C.writeChan (dIns ! 0) (Data x)
            return $ Just ()
          proc _ Stop _ = return Nothing
          init _ = return ()

keyBy :: (Hashable k, Eq k) => Int -> (a -> k) -> (a -> b) -> Stream a -> Stream (k, b)
keyBy outDeg key value = make1toManyOperator outDeg proc init
    where proc () (Data x) dIns = do
            let k = key x
                v = value x
                i = hash k `mod` V.length dIns
            C.writeChan (dIns ! i) (Data (k, v))
            return $ Just ()
          proc () Stop _ = return Nothing
          init _ = return ()

--
-- Evaluating streams
--

runStream :: Stream a -> IO ()
runStream inStream = do
    procs <- drain inStream
    mvars <- sequence $ replicate (length procs) newEmptyMVar
    threads <- mvars `zip` procs
                & fmap (\(mv, p) -> forkFinally p (\_ -> putMVar mv ()))
                & sequence
    fmap readMVar mvars & sequence
    return ()
    where drain inStream = do
              (sOuts, sProcs) <- getStreamOP inStream
              return (sProcs ++ newProcs sOuts)
              where newProcs sOuts = sOuts & fmap go & V.toList
                    go srcChan = do
                      x <- C.readChan srcChan
                      case x of
                          Data _ -> go srcChan
                          Stop -> return ()

-- 
-- Note that the resulting list is reversed
--
toList :: Stream a -> IO [a]
toList inStream = do
    (outs, procs) <- getStreamOP $ gather inStream
    threads <- procs & fmap forkIO & sequence
    go (outs ! 0) []
    where go srcChan xs = do
            msg <- C.readChan srcChan
            case msg of
                Data x -> go srcChan (x:xs)
                Stop -> return xs

-- Can we rewrite toList using something from Control.Monad.Loops?
toList' :: Stream a -> IO [a]
toList' inStream = do
    (outs, procs) <- getStreamOP $ gather inStream
    threads <- procs & fmap forkIO & sequence
    unfoldM $ go (outs ! 0)
    where go srcChan = do
            msg <- C.readChan srcChan
            case msg of
                Data x -> return $ Just x
                Stop -> return $ Nothing


-- 
-- Utilities
--

printCSV :: Stream [String] -> Stream ()
printCSV = mapM (putStrLn . (L.intercalate ","))

del :: Int -> a -> IO a
del n x = threadDelay (n * 1000000) >> return x

dupChannel :: InChannel a -> IO (OutChannel a)
dupChannel = V.mapM C.dupChan

broadcastChannel :: Int -> Int -> IO (InChannel a, [OutChannel a])
broadcastChannel n m = do
    (ins, firstOut) <- newChannel n
    outs <- replicate (m - 1) (dupChannel ins) & sequence
    return (ins, firstOut : outs)

-- 
-- Runs a list of queries in parallel over copies of the same input and
-- collects the results in a single stream
--
-- Note that we assume everything has the same degree of parallelism...
--
parallel :: [Stream a -> Stream b] -> Stream a -> Stream b
parallel queries inStream = do
    (sOuts, sProcs) <- getStreamOP inStream
    let n = V.length sOuts
    (dIns, dOuts) <- newChannel n

    mvarss <- replicate (length queries) (replicate n newEmptyMVar & sequence) & sequence


    (bcIns, bcOuts) <- broadcastChannel n (length queries)

    -- We have to go from sOuts :: OutChannel a (aka Vector (OutChan (Message a))) to list of (length query) of OutChannel a's
    -- We have C.dupChan :: InChan a -> IO (OutChan a) and C.newChan :: Int -> IO (InChan a, OutChan a)
    -- We also have newChannel :: Int -> IO (InChannel a, OutChannel a)

    -- queryProcs needs a list of OutChannel a as first arg
    newProcs <- queryProcs bcOuts dIns mvarss

    newStream dOuts (sProcs
        ++ broadcaster sOuts bcIns
        ++ newProcs
        ++ [terminatorProc dIns mvarss])

    where broadcaster sOuts bcIns =
            V.zip sOuts bcIns & V.toList & fmap go
            where go (srcChan, dstChan) = do
                    msg <- C.readChan srcChan
                    case msg of
                        Data x -> do
                            C.writeChan dstChan (Data x)
                            go (srcChan, dstChan)
                        Stop -> do
                            C.writeChan dstChan Stop
                            return ()

          queryProcs cloneOuts dIns mvarss = do
            newProcs <- zip3 queries cloneOuts mvarss & fmap runQuery & sequence
            return $ concat newProcs
            where runQuery (query, cloneOut, mvars) = do
                    (sOuts', sProcs') <- newStream cloneOut [] & query & getStreamOP
                    return $ sProcs' ++ (V.zip3 sOuts' dIns (V.fromList mvars) & V.toList & fmap go)
                    where go (srcChan, dstChan, mvar) = do
                            msg <- C.readChan srcChan
                            case msg of
                                Data x -> do
                                    C.writeChan dstChan (Data x)
                                    go (srcChan, dstChan, mvar)
                                Stop -> do
                                    putMVar mvar ()
                                    return ()

          terminatorProc dIns mvars = do
            fmap (\mvs -> fmap readMVar mvs & sequence) mvars & sequence
            fmap (\c -> C.writeChan c Stop) dIns & sequence
            return ()
