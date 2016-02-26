/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bugull.redis.performance;

import com.bugull.redis.RedisConnection;
import com.bugull.redis.listener.TopicListener;
import com.bugull.redis.mq.MQClient;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;

/**
 *
 * @author frankwen
 */
public class PerformanceTest {
    
    RedisConnection conn;
    MQClient client;
    
    
    AtomicLong count = new AtomicLong(0);
    
    long beginTime, endTime;
    
    @Test
    public void test() throws Exception {
        conn = RedisConnection.getInstance();
        conn.setHost("127.0.0.1");
        conn.setPassword("foobared");
        conn.connect();
        
        client = conn.getMQClient();
        
        testSubscribe();
        
        Thread.sleep(1000);
        
        testPublish();
        
        Thread.sleep(10L * 1000L);
        
        conn.disconnect();
    }
    
    
    public void testPublish() throws Exception {
        
        for(int i=0; i<10; i++){
            PublishTask task = new PublishTask();
            new Thread(task).start();
        }
    }
    
    class PublishTask implements Runnable {

        @Override
        public void run() {
            try{
                for(int i=0; i<5000; i++){
                    client.publish("my_topic", "hello".getBytes());
                }
            }catch(Exception ex){
                ex.printStackTrace();
            }
        }
        
    }
    
    public void testSubscribe() throws Exception {
        
        TopicListener listener = new TopicListener(){
            @Override
            public void onTopicMessage(String topic, byte[] message) {
                long value = count.incrementAndGet();
                if(value==1){
                    beginTime = System.currentTimeMillis();
                }else if(value==50000){
                    endTime = System.currentTimeMillis();
                    System.out.println("use " + (endTime - beginTime) + "ms to receive 10000 messages.");
                }
            }
        };
        
        client.setTopicListener(listener);
        
        client.subscribe("my_topic");
        
        System.out.println("Subscribe success! Listening message...");
    }
    
}
