{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings #-}

module Manifest.Redis (

    Redis

  , redis
  , R.ConnectInfo

  , RedisManifestFailure(..)

  ) where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as B8
import qualified Database.Redis as R
import Control.Exception
import Control.Monad.Trans.Class
import Control.Monad.Trans.Reader
import Control.Monad.Trans.Except
import Data.List (sortBy)
import Data.TypeNat.Vect
import Manifest.Manifest

data Redis a = Redis {
    redisConnectInfo :: R.ConnectInfo
  }

redis :: R.ConnectInfo -> Redis a
redis = Redis

data RedisManifestFailure
  = RedisManifestNoConnection
  | RedisManifestReadFailure
  | RedisManifestWriteFailure
  | RedisManifestDeleteFailure
  | RedisManifestWeirdResult
  | RedisManifestOtherFailure
  deriving (Show)

sortByKey :: [(BS.ByteString, BS.ByteString)] -> [BS.ByteString]
sortByKey = map snd . sortBy comparator
  where
    comparator (x,_) (y,_) = compare x y

vectToRedisValues :: Vect BS.ByteString n -> [(BS.ByteString, BS.ByteString)]
vectToRedisValues = vectToRedisValues' 1
  where
    vectToRedisValues' :: Int -> Vect BS.ByteString n -> [(BS.ByteString, BS.ByteString)]
    vectToRedisValues' i vect = case vect of
        VNil -> []
        VCons bs v -> (B8.pack (show i), bs) : vectToRedisValues' (i+1) v

instance Manifest Redis where

  type ManifestMonad Redis = ExceptT RedisManifestFailure R.Redis
  type PeculiarManifestFailure Redis = RedisManifestFailure

  manifestRead proxy (proxy' :: u n) key = do
      hashmap <- lift (R.hgetall key)
      case hashmap of
        Left _ -> throwE RedisManifestWeirdResult
        Right [] -> return $ Right Nothing
        Right bss -> do
          let sorted = sortByKey bss
          case listToVect sorted of
            Nothing -> throwE RedisManifestReadFailure
            Just vect -> return $ Right (Just vect)

  manifestWrite proxy proxy' key valueVect = do
      let values = vectToRedisValues valueVect
      result <- (lift) (R.hmset key values)
      case result of
        Left _ -> throwE RedisManifestWeirdResult
        Right R.Ok -> return ()
        Right _ -> throwE RedisManifestWeirdResult

  manifestDelete proxy proxy' key = do
      -- First we have to get the current key.
      let readAction = manifestRead proxy proxy' key
      readOutcome <- catchE readAction catchReadFailure
      result <- (lift) (R.del [key])
      case result of
        Left _ -> throwE RedisManifestWeirdResult
        Right _ -> case readOutcome of
          Right Nothing -> return $ Right Nothing
          -- ^ Key not found
          Right (Just x) -> return $ Right (Just x)
          -- ^ Key found, value recovered
          Left () -> return $ Left ()
          -- ^ Key found, value not recovered.

    where

      catchReadFailure :: RedisManifestFailure -> ExceptT RedisManifestFailure R.Redis (Either () (Maybe (Vect BS.ByteString n)))
      catchReadFailure x = return $ Left ()

  manifestRun r@(Redis connInfo) action = do
      conn <- R.connect connInfo
      -- ^ I believe R.connect will never throw an exception; it just creates
      --   a resource pool!
      eitherException <- try $ R.runRedis conn (runExceptT action)
      -- ^ TODO exception handling is definitely too coarse here.
      --   Not sure how to isolate connection failures.
      case eitherException of
        Left (exception :: SomeException) -> return (Left RedisManifestNoConnection, r)
        Right outcome -> case outcome of
          Left failure -> return (Left failure, r)
          Right value -> return (Right value, r)
