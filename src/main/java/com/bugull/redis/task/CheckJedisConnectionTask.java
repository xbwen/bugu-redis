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
package com.bugull.redis.task;

import com.bugull.redis.RedisConnection;
import com.bugull.redis.exception.RedisException;
import com.bugull.redis.mq.MQClient;
import com.bugull.redis.utils.Constant;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import redis.clients.jedis.Jedis;

/**
 *
 * @author Frank Wen(xbwen@hotmail.com)
 */
public class CheckJedisConnectionTask implements Runnable {
    
    private Jedis jedis;
    private String topic;
    
    public CheckJedisConnectionTask(Jedis jedis, String topic){
        this.jedis = jedis;
        this.topic = topic;
    }
    
    @Override
    public void run() {
        MQClient client = RedisConnection.getInstance().getMQClient();
        
        try {
            client.publish(topic, Constant.HEART_BEAT);
        } catch (RedisException ex) {
            ex.printStackTrace();
        }
        
        int idleTime = client.getIdleTime();
        ConcurrentMap<String, Long> lastMessageTime = client.getLastMessageTime();
        Long last = lastMessageTime.get(topic);
        if(last==null){
            lastMessageTime.put(topic, System.currentTimeMillis());
        }
        // 2 times idleTime, plus 3 seconds offset
        else if( (last + (2 * idleTime * 1000) + 3000) < System.currentTimeMillis() ){
            //disconnect jedis, the loop in SubscribeTopicTask will continue again
            lastMessageTime.remove(topic);
            jedis.disconnect();
        }
        
    }
    
}
