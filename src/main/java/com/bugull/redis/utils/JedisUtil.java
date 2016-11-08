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

package com.bugull.redis.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;

/**
 *
 * @author Frank Wen(xbwen@hotmail.com)
 */
public final class JedisUtil {
    
    public static void returnToPool(JedisPool pool, Jedis jedis){
        try{
            pool.returnResource(jedis);
        }catch(JedisException ex){
            ex.printStackTrace();
        }
    }
    
    public static boolean isEmpty(byte[] bytes){
        return bytes==null || bytes.length==0;
    }
    
    public static boolean isEmpty(String s){
        return s==null || s.trim().length()==0;
    }

}
