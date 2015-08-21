/*
 * Copyright (c) www.bugull.com
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bugull.redis.recipes;

import com.bugull.redis.RedisConnection;
import com.bugull.redis.exception.RedisException;
import com.bugull.redis.utils.JedisUtil;
import java.util.Date;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;

/**
 * Cache data will expire
 * 
 * @author Frank Wen(xbwen@hotmail.com)
 */
public class CacheRecipe extends StringRecipe {
    
    public void put(String key, int expire, String data) throws RedisException {
        put(key.getBytes(), expire, data.getBytes());
    }
    
    public void put(String key, Date expireAt, String data) throws RedisException {
        put(key.getBytes(), expireAt, data.getBytes());
    }
    
    public void put(byte[] key, int expire, byte[] data) throws RedisException {
        JedisPool pool = RedisConnection.getInstance().getPool();
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
            Transaction tx = jedis.multi();
            tx.set(key, data);
            tx.expire(key, expire);
            tx.exec();
        }catch(Exception ex){
            throw new RedisException(ex.getMessage(), ex);
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
    }
    
    public void put(byte[] key, Date expireAt, byte[] data) throws RedisException {
        JedisPool pool = RedisConnection.getInstance().getPool();
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
            Transaction tx = jedis.multi();
            tx.set(key, data);
            tx.expireAt(key, expireAt.getTime());
            tx.exec();
        }catch(Exception ex){
            throw new RedisException(ex.getMessage(), ex);
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
    }

}
