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

package com.bugull.redis.mq;

import com.bugull.redis.RedisConnection;
import com.bugull.redis.utils.Constant;
import com.bugull.redis.exception.RedisException;
import com.bugull.redis.listener.QueueListener;
import com.bugull.redis.listener.TopicListener;
import com.bugull.redis.task.BlockedTask;
import com.bugull.redis.task.ConsumeQueueTask;
import com.bugull.redis.task.SubscribeTopicTask;
import com.bugull.redis.utils.JedisUtil;
import com.bugull.redis.utils.ThreadUtil;
import java.util.Date;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;

/**
 * MQ client
 * 
 * @author Frank Wen(xbwen@hotmail.com)
 */
public class MQClient {
    
    private JedisPool pool;
    
    private TopicListener topicListener;
    
    private final ConcurrentMap<String, BlockedTask> blockedTasks = new ConcurrentHashMap<String, BlockedTask>();
    
    private final ConcurrentMap<String, ExecutorService> topicServices = new ConcurrentHashMap<String, ExecutorService>();
    
    private final ConcurrentMap<String, ExecutorService> queueServices = new ConcurrentHashMap<String, ExecutorService>();

    public MQClient(){
        this.pool = RedisConnection.getInstance().getPool();
    }
    
    public void publish(String topic, byte[] message) throws RedisException {
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
            jedis.publish(topic.getBytes(), message);
        }catch(Exception ex){
            //Note: catch Exception here, because there are many runtime exception in Jedis.
            //Following code is same like this.
            throw new RedisException(ex.getMessage(), ex);
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
    }
    
    public void publishRetain(String topic, byte[] message) throws RedisException {
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
            Transaction tx = jedis.multi();
            tx.publish(topic.getBytes(), message);
            tx.set((Constant.RETAIN + topic).getBytes(), message);
            tx.exec();
        }catch(Exception ex){
            throw new RedisException(ex.getMessage(), ex);
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
    }
    
    public void clearRetainMessage(String... topics) throws RedisException {
        Jedis jedis = null;
        int len = topics.length;
        byte[][] keys = new byte[len][];
        try{
            for(int i=0; i< len; i++){
                keys[i] = (Constant.RETAIN + topics[i]).getBytes();
            }
            jedis = pool.getResource();
            jedis.del(keys);
        }catch(Exception ex){
            throw new RedisException(ex.getMessage(), ex);
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
    }
    
    public void subscribe(String... topics) {
        for(String topic : topics){
            ExecutorService es = topicServices.get(topic);
            if(es == null){
                //use single thread executor to make sure the thread never crash
                es = Executors.newSingleThreadExecutor();
                ExecutorService temp = topicServices.putIfAbsent(topic, es);
                if(temp == null){
                    SubscribeTopicTask task = new SubscribeTopicTask(topicListener, pool, topic.getBytes());
                    es.execute(task);
                    blockedTasks.putIfAbsent(topic, task);
                }
            }
        }
    }
    
    public void unsubscribe(String... topics) throws RedisException {
        for(String topic : topics){
            try{
                topicListener.unsubscribe(topic.getBytes());
            }catch(Exception ex){
                throw new RedisException(ex.getMessage(), ex);
            }
            stopTopicTask(topic);
        }
    }
    
    public void produce(String queue, byte[]... messages) throws RedisException {
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
            for(byte[] msg : messages){
                String id = UUID.randomUUID().toString();
                byte[] msgId = (Constant.MSG + id).getBytes();
                Transaction tx = jedis.multi();
                tx.lpush(queue, id);
                tx.set(msgId, msg);
                tx.exec();
            }
        }catch(Exception ex){
            throw new RedisException(ex.getMessage(), ex);
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
    }
    
    public void produce(String queue, int expire, byte[]... messages) throws RedisException {
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
            for(byte[] msg : messages){
                String id = UUID.randomUUID().toString();
                byte[] msgId = (Constant.MSG + id).getBytes();
                Transaction tx = jedis.multi();
                tx.lpush(queue, id);
                tx.set(msgId, msg);
                tx.expire(msgId, expire);
                tx.exec();
            }
        }catch(Exception ex){
            throw new RedisException(ex.getMessage(), ex);
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
    }
    
    public void produce(String queue, Date expireAt, byte[]... messages) throws RedisException {
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
            for(byte[] msg : messages){
                String id = UUID.randomUUID().toString();
                byte[] msgId = (Constant.MSG + id).getBytes();
                Transaction tx = jedis.multi();
                tx.lpush(queue, id);
                tx.set(msgId, msg);
                tx.expireAt(msgId, expireAt.getTime());
                tx.exec();
            }
        }catch(Exception ex){
            throw new RedisException(ex.getMessage(), ex);
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
    }
    
    public void produceUrgency(String queue, byte[]... messages) throws RedisException {
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
            for(byte[] msg : messages){
                String id = UUID.randomUUID().toString();
                byte[] msgId = (Constant.MSG + id).getBytes();
                Transaction tx = jedis.multi();
                tx.rpush(queue, id);
                tx.set(msgId, msg);
                tx.exec();
            }
        }catch(Exception ex){
            throw new RedisException(ex.getMessage(), ex);
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
    }
    
    public void produceUrgency(String queue, int expire, byte[]... messages) throws RedisException {
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
            for(byte[] msg : messages){
                String id = UUID.randomUUID().toString();
                byte[] msgId = (Constant.MSG + id).getBytes();
                Transaction tx = jedis.multi();
                tx.rpush(queue, id);
                tx.set(msgId, msg);
                tx.expire(msgId, expire);
                tx.exec();
            }
        }catch(Exception ex){
            throw new RedisException(ex.getMessage(), ex);
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
    }
    
    public void produceUrgency(String queue, Date expireAt, byte[]... messages) throws RedisException {
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
            for(byte[] msg : messages){
                String id = UUID.randomUUID().toString();
                byte[] msgId = (Constant.MSG + id).getBytes();
                Transaction tx = jedis.multi();
                tx.rpush(queue, id);
                tx.set(msgId, msg);
                tx.expireAt(msgId, expireAt.getTime());
                tx.exec();
            }
        }catch(Exception ex){
            throw new RedisException(ex.getMessage(), ex);
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
    }
    
    public void consume(QueueListener listener, String... queues){
        for(String queue : queues){
            ExecutorService es = queueServices.get(queue);
            if(es == null){
                //use single thread executor to make sure the thread never crash
                es = Executors.newSingleThreadExecutor();
                ExecutorService temp = queueServices.putIfAbsent(queue, es);
                if(temp == null){
                    ConsumeQueueTask task = new ConsumeQueueTask(listener, pool, queue);
                    es.execute(task);
                    blockedTasks.putIfAbsent(queue, task);
                }
            }
        }
    }
    
    public void clearQueue(String... queues) throws RedisException {
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
            for(String queue : queues){
                long size = jedis.llen(queue);
                for(long i=0; i<size; i++){
                    String id = jedis.rpop(queue);
                    if(id != null){
                        jedis.del((Constant.MSG + id).getBytes());
                    }
                }
            }
        }catch(Exception ex){
            throw new RedisException(ex.getMessage(), ex);
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
    }
    
    public void retainQueue(String queue, long retainSize) throws RedisException {
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
            long size = jedis.llen(queue);
            long count = size - retainSize;
            for(long i=0; i<count; i++){
                String id = jedis.rpop(queue);
                if(id != null){
                    jedis.del((Constant.MSG + id).getBytes());
                }
            }
        }catch(Exception ex){
            throw new RedisException(ex.getMessage(), ex);
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
    }
    
    public long getQueueSize(String queue) throws RedisException {
        long size = 0;
        Jedis jedis = null;
        try{
            jedis = pool.getResource();
            size = jedis.llen(queue);
        }catch(Exception ex){
            throw new RedisException(ex.getMessage(), ex);
        }finally{
            JedisUtil.returnToPool(pool, jedis);
        }
        return size;
    }

    public void setTopicListener(TopicListener topicListener) {
        this.topicListener = topicListener;
    }
    
    private void stopConsume(String... queues){
        for(String queue : queues){
            BlockedTask task = blockedTasks.get(queue);
            if(task != null){
                task.setStopped(true);
                try{
                    task.getJedis().disconnect();
                }catch(Exception ex){
                    ex.printStackTrace();
                }
                blockedTasks.remove(queue);
            }
            ExecutorService es = queueServices.get(queue);
            if(es != null){
                queueServices.remove(queue);
                ThreadUtil.safeClose(es);
            }
        }
    }
    
    public void stopAllConsume(){
        Set<String> set = queueServices.keySet();
        for(String queue : set){
            stopConsume(queue);
        }
    }
    
    public void stopAllTopicTask(){
        Set<String> set = topicServices.keySet();
        for(String topic : set){
            stopTopicTask(topic);
        }
        topicListener.closeScheduler();
    }
    
    private void stopTopicTask(String topic){
        BlockedTask task = blockedTasks.get(topic);
        if(task != null){
            task.setStopped(true);
            try{
                task.getJedis().disconnect();
            }catch(Exception ex){
                ex.printStackTrace();
            }
            blockedTasks.remove(topic);
        }
        ExecutorService es = topicServices.get(topic);
        if(es != null){
            topicServices.remove(topic);
            ThreadUtil.safeClose(es);
        }
    }
    
    public BlockedTask getBlockedTask(String s){
        return blockedTasks.get(s);
    }
    
}
