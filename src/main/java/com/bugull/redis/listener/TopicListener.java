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
import com.bugull.redis.mq.MQClient;
import com.bugull.redis.task.BlockedTask;
import com.bugull.redis.task.SubscribeTopicTask;
import com.bugull.redis.utils.Constant;
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
    
    protected final ConcurrentMap<String, ScheduledFuture> timeoutMap = new ConcurrentHashMap<String, ScheduledFuture>();
    protected final ConcurrentMap<String, ScheduledFuture> timerMap = new ConcurrentHashMap<String, ScheduledFuture>();
    
    protected final ScheduledExecutorService timeoutScheduler = Executors.newSingleThreadScheduledExecutor();
    protected final ScheduledExecutorService timerScheduler = Executors.newSingleThreadScheduledExecutor();
    
    public abstract void onTopicMessage(String topic, byte[] message);
    
    @Override
    public void onMessage(byte[] channel, byte[] message){
        if(JedisUtil.isEmpty(channel) || JedisUtil.isEmpty(message)){
            return;
        }
        else if(message.length==1 && Arrays.equals(message, Constant.HEART_BEAT)){
            //received heartbeat
            String s = new String(channel);
            removeTimeout(s);
            setTimer(s);
            setTimeout(s, 2);
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
        //publish a heartbeat
        try{
            jedis = pool.getResource();
            jedis.publish(channel, Constant.HEART_BEAT);
        }catch(Exception ex){
            ex.printStackTrace();
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
        //set a timeout
        setTimeout(s, 1);
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
    
    private void setTimer(final String topic){
        Runnable task = new Runnable(){
            @Override
            public void run() {
                //publish a heartbeat
                JedisPool pool = RedisConnection.getInstance().getPool();
                Jedis jedis = null;
                try{
                    jedis = pool.getResource();
                    jedis.publish(topic.getBytes(), Constant.HEART_BEAT);
                }catch(Exception ex){
                    ex.printStackTrace();
                }finally{
                    JedisUtil.returnToPool(pool, jedis);
                }
            }
        };
        ScheduledFuture future = timerScheduler.schedule(task, Constant.HEART_BEAT_TIME, TimeUnit.SECONDS);
        ScheduledFuture temp = timerMap.put(topic, future);
        if(temp != null){
            temp.cancel(true);
        }
    }
    
    private void setTimeout(final String topic, int times){
        Runnable task = new Runnable(){
            @Override
            public void run() {
                System.out.println("timeout!!!");
                timeoutMap.remove(topic);
                //close the jedis connection
                MQClient client = RedisConnection.getInstance().getMQClient();
                BlockedTask task = client.getBlockedTask(topic);
                if(task!=null && (task instanceof SubscribeTopicTask)){
                    task.getJedis().disconnect();
                }
            }
        };
        ScheduledFuture future = timeoutScheduler.schedule(task, times * Constant.HEART_BEAT_TIME, TimeUnit.SECONDS);
        ScheduledFuture temp = timeoutMap.put(topic, future);
        if(temp != null){
            temp.cancel(true);
        }
    }
    
    private void removeTimeout(String topic){
        ScheduledFuture temp = timeoutMap.remove(topic);
        if(temp != null){
            temp.cancel(true);
        }
    }
    
    public void closeScheduler(){
        ThreadUtil.safeClose(timeoutScheduler);
        ThreadUtil.safeClose(timerScheduler);
    }

}
