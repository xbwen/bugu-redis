/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bugull.redis;

import com.bugull.redis.listener.TopicListener;
import com.bugull.redis.mq.MQClient;
import org.junit.Test;

/**
 *
 * @author frankwen
 */
public class RemoteTest {
    
    @Test
    public void test() throws Exception {
        RedisConnection conn = RedisConnection.getInstance();
        conn.setHost("remote_ip");
        conn.setPassword("remote_passwd");
        conn.connect();
        
        MQClient client = conn.getMQClient();
        client.setTopicListener(new TopicListener(){
            @Override
            public void onTopicMessage(String topic, byte[] message) {
                System.out.println("topic: " + topic);
                System.out.println("msg: " + new String(message));
            }
        });
        
        client.subscribe("topic:web");
        
        Thread.sleep(3 * 60 * 1000);
        
        conn.disconnect();
    }
    
}
