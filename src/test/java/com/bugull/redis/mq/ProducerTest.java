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
import org.junit.Test;

/**
 *
 * @author Frank Wen(xbwen@hotmail.com)
 */
public class ProducerTest {
    
    @Test
    public void testProduce() throws Exception {
        RedisConnection conn = RedisConnection.getInstance();
        conn.setHost("127.0.0.1");
        conn.setPassword("foobared");
        conn.connect();
        
        MQClient client = conn.getMQClient();
        byte[] msg = new byte[]{0x31, 0x32, 0x33};
        client.produce("queue1", msg);
        client.produce("queue2", msg);
        
        Thread.sleep(1000L);
        
        conn.disconnect();
    }

}
