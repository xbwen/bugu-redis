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

import com.bugull.redis.mq.MQClient;
import com.bugull.redis.RedisConnection;
import com.bugull.redis.utils.Constant;
import com.bugull.redis.exception.RedisException;
import com.bugull.redis.utils.JedisUtil;
import com.bugull.redis.utils.ThreadUtil;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 *
 * @author Frank Wen(xbwen@hotmail.com)
 */
public abstract class TopicListener extends BinaryJedisPubSub {
    
    protected final ConcurrentMap<String, ScheduledFuture> map = new ConcurrentHashMap<String, ScheduledFuture>();
    
    protected final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    
    /**
     * use a timer to re-subscribe
     * @param topic
     * @param pattern 
     */
    public void addTimer(final String topic){
        Runnable task = new Runnable(){
            @Override
            public void run() {
                map.remove(topic);
                MQClient client = RedisConnection.getInstance().getMQClient();
                try{
                    client.unsubscribe(topic);
                }catch(RedisException ex){
                    ex.printStackTrace();
                }
                client.subscribe(topic);
            }
        };
        ScheduledFuture future = scheduler.schedule(task, Constant.SUBSCRIBE_TIMEOUT, TimeUnit.SECONDS);
        ScheduledFuture temp = map.putIfAbsent(topic, future);
        if(temp != null){
            future.cancel(true);
        }
    }
    
    public void removeTimer(String topic){
        ScheduledFuture future = map.remove(topic);
        if(future != null){
            future.cancel(true);
        }
    }
    
    public void closeAllTimer(){
        ThreadUtil.safeClose(scheduler);
    }
    
    public abstract void onTopicMessage(String topic, byte[] message);
    
    @Override
    public void onMessage(byte[] channel, byte[] message){
        if(JedisUtil.isEmpty(channel) || JedisUtil.isEmpty(message)){
            return;
        }
        //filter the heartbeat message
        if(message.length==1 && Arrays.equals(message, Constant.HEART_BEAT)){
            MQClient client = RedisConnection.getInstance().getMQClient();
            ConcurrentMap<String, Long> lastMessageTime = client.getLastMessageTime();
            lastMessageTime.put(new String(channel), System.currentTimeMillis());
        }else{
            onTopicMessage(new String(channel), message);
        }
    }

    @Override
    public void onSubscribe(byte[] channel, int subscribedChannels){
        if(JedisUtil.isEmpty(channel)){
            return;
        }
        String s = new String(channel);
        removeTimer(s);
        //get retain message
        RedisConnection conn = RedisConnection.getInstance();
        JedisPool pool = conn.getPool();
        Jedis jedis = null;
        byte[] retainMessage = null;
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
