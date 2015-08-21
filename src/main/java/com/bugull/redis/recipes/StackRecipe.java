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
 * FILO Stack
 * 
 * @author Frank Wen(xbwen@hotmail.com)
 */
public class StackRecipe extends ListRecipe {
    
    public void push(String queue, String element) throws RedisException {
        push(queue.getBytes(), element.getBytes());
    }
    
    public void push(byte[] queue, byte[] element) throws RedisException {
        JedisPool pool = RedisConnection.getInstance().getPool();
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
            jedis.lpush(queue, element);
        }catch(Exception ex){
            throw new RedisException(ex.getMessage(), ex);
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
    }
    
    public String pop(String queue) throws RedisException {
        byte[] bytes = pop(queue.getBytes());
        if(bytes==null){
            return null;
        }else{
            return new String(bytes);
        }
    }
    
    public byte[] pop(byte[] queue) throws RedisException {
        JedisPool pool = RedisConnection.getInstance().getPool();
        Jedis jedis = null;
        byte[] result = null;
        try{
            jedis = pool.getResource();
            result = jedis.lpop(queue);
        }catch(Exception ex){
            throw new RedisException(ex.getMessage(), ex);
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
        return result;
    }

}
