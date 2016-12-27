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


package com.bugull.redis.concurrent;

import com.bugull.redis.performance.*;
import com.bugull.redis.RedisConnection;
import com.bugull.redis.mq.MQClient;
import org.junit.Test;

/**
 *
 * @author Frank Wen(xbwen@hotmail.com)
 */
public class PublisherTest {
    
    RedisConnection conn;
    MQClient client;
    
    //@Test
    public void testPublish() throws Exception {
        conn = RedisConnection.getInstance();
        conn.setHost("127.0.0.1");
        conn.setPassword("foobared");
        conn.connect();
        
        client = conn.getMQClient();
        for(int i=0; i<10; i++){
            PublishTask task = new PublishTask();
            new Thread(task).start();
        }
        
        Thread.sleep(20L * 1000L);
        
        conn.disconnect();
    }
    
    class PublishTask implements Runnable {

        @Override
        public void run() {
            try{
                client.publish("my_topic", "hello".getBytes());
            }catch(Exception ex){
                ex.printStackTrace();
            }
        }
        
    }

}
