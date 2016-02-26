/*
 * Copyright (c) www.bugull.com
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
public abstract class AbstractListRecipe extends BaseRecipe {
    
    public long getSize(String listName) throws RedisException {
        return getSize(listName.getBytes());
    }
    
    public long getSize(byte[] listName) throws RedisException {
        long size = 0;
        JedisPool pool = RedisConnection.getInstance().getPool();
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
            size = jedis.llen(listName);
        }catch(Exception ex){
            throw new RedisException(ex.getMessage(), ex);
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
        return size;
    }

}
