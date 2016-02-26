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
import java.util.List;
import java.util.UUID;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;

/**
 * Distributed lock. Timeout value in seconds
 * 
 * @author Frank Wen(xbwen@hotmail.com)
 */
public class LockRecipe extends AbstractStringRecipe {
    
    public String acquireLock(String lockName) throws RedisException {
        return acquireLock(lockName, 5);
    }
    
    public String acquireLock(String lockName, int acquireTimeout) throws RedisException {
        return acquireLock(lockName, acquireTimeout, 10);
    }
    
    public String acquireLock(String lockName, int acquireTimeout, int lockTimeout) throws RedisException {
        String result = null;
        String uuid = UUID.randomUUID().toString();
        long endTime = System.currentTimeMillis() + (acquireTimeout * 1000);
        while(System.currentTimeMillis() < endTime){
            JedisPool pool = RedisConnection.getInstance().getPool();
            Jedis jedis = null;
            String status = null;
            try{
                jedis = pool.getResource();
                status = jedis.set(lockName, uuid, "NX", "EX", lockTimeout);
            }catch(Exception ex){
                throw new RedisException(ex.getMessage(), ex);
            }finally{
                JedisUtil.returnToPool(pool, jedis);
            }
            if(status!=null && status.equals("OK")){
                result = uuid;
                break;
            }else{
                try {
                    Thread.sleep(10);
                } catch (InterruptedException ex) {
                    //ignore ex
                }
            }
        }
        return result;
    }
    
    public boolean releaseLock(String lockName, String lockValue) throws RedisException {
        if(lockValue == null){
            return false;
        }
        boolean result = false;
        JedisPool pool = RedisConnection.getInstance().getPool();
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
            jedis.watch(lockName);
            String value = jedis.get(lockName);
            if(value!=null && value.equals(lockValue)){
                Transaction tx = jedis.multi();
                tx.del(lockName);
                List list = tx.exec();
                if(list != null){
                    result = true;
                }
            }
        }catch(Exception ex){
            throw new RedisException(ex.getMessage(), ex);
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
        return result;
    }

}
