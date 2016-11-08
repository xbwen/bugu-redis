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


package com.bugull.redis.performance;

import com.bugull.redis.RedisConnection;
import com.bugull.redis.listener.TopicListener;
import com.bugull.redis.mq.MQClient;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;

/**
 *
 * @author Frank Wen(xbwen@hotmail.com)
 */
public class SubscriberTest {
    
    AtomicLong count = new AtomicLong(0);
    
    long beginTime, endTime;
    
    @Test
    public void testSubscribe() throws Exception {
        RedisConnection conn = RedisConnection.getInstance();
        conn.setHost("127.0.0.1");
        conn.setPassword("foobared");
        conn.connect();
        
        MQClient client = conn.getMQClient();
        
        TopicListener listener = new TopicListener(){
            @Override
            public void onTopicMessage(String topic, byte[] message) {
                long value = count.incrementAndGet();
                if(value==1){
                    beginTime = System.currentTimeMillis();
                }else if(value==30000){
                    endTime = System.currentTimeMillis();
                    System.out.println("use " + (endTime - beginTime) + "ms to receive 30000 messages.");
                }
            }
        };
        
        client.setTopicListener(listener);
        
        client.subscribe("my_topic");
        
        System.out.println("Subscribe success! Listening message...");
        
        Thread.sleep(30L * 1000L);
        
        conn.disconnect();
    }

}
