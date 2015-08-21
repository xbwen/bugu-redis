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
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 *
 * @author Frank Wen(xbwen@hotmail.com)
 */
public class CounterRecipe extends StringRecipe {
    
    public long getCount(String key) throws RedisException {
        String s = super.get(key);
        if(s == null){
            return 0;
        }
        return Long.parseLong(s);
    }
    
    public long increase(String key) throws RedisException {
        long count = 0;
        JedisPool pool = RedisConnection.getInstance().getPool();
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
            count = jedis.incr(key);
        }catch(Exception ex){
            throw new RedisException(ex.getMessage(), ex);
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
        return count;
    }
    
    public long decrease(String key) throws RedisException {
        long count = 0;
        JedisPool pool = RedisConnection.getInstance().getPool();
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
            count = jedis.decr(key);
        }catch(Exception ex){
            throw new RedisException(ex.getMessage(), ex);
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
        return count;
    }

}
