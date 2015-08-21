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
import com.bugull.redis.listener.QueueListener;
import org.junit.Test;

/**
 *
 * @author Frank Wen(xbwen@hotmail.com)
 */
public class ConsumerTest {
    
    @Test
    public void testConsume() throws Exception {
        RedisConnection conn = RedisConnection.getInstance();
        conn.setHost("127.0.0.1");
        conn.setPassword("foobared");
        conn.connect();
        
        MQClient client = conn.getMQClient();
        
        QueueListener listener = new QueueListener(){
            @Override
            public void onQueueMessage(String queue, byte[] message) {
                System.out.println("queue: " + queue);
                for(byte b : message){
                    System.out.println(b);
                }
            }
        };
        
        client.consume(listener, "queue1", "queue2");
        
        Thread.sleep(30L * 1000L);
        
        conn.disconnect();
    }

}
