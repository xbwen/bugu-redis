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

package com.bugull.redis.listener;

import com.bugull.redis.RedisConnection;
import com.bugull.redis.utils.Constant;
import com.bugull.redis.utils.JedisUtil;
import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 *
 * @author Frank Wen(xbwen@hotmail.com)
 */
public abstract class TopicListener extends BinaryJedisPubSub {
    
    public abstract void onTopicMessage(String topic, byte[] message);
    
    @Override
    public void onMessage(byte[] channel, byte[] message){
        if(JedisUtil.isEmpty(channel) || JedisUtil.isEmpty(message)){
            return;
        }
        onTopicMessage(new String(channel), message);
    }

    @Override
    public void onSubscribe(byte[] channel, int subscribedChannels){
        if(JedisUtil.isEmpty(channel)){
            return;
        }
        //get retain message
        JedisPool pool = RedisConnection.getInstance().getPool();
        Jedis jedis = null;
        byte[] retainMessage = null;
        String s = new String(channel);
        try{
            jedis = pool.getResource();
            retainMessage = jedis.get((Constant.RETAIN + s).getBytes());
        }catch(Exception ex){
            ex.printStackTrace();
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
        if(retainMessage != null){
            onTopicMessage(s, retainMessage);
        }
    }
    
    @Override
    public void onPMessage(byte[] pattern, byte[] channel, byte[] message){
        
    }

    @Override
    public void onUnsubscribe(byte[] channel, int subscribedChannels){
        
    }

    @Override
    public void onPUnsubscribe(byte[] pattern, int subscribedChannels){
        
    }

    @Override
    public void onPSubscribe(byte[] pattern, int subscribedChannels){
        
    }

}
