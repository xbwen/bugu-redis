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

package com.bugull.redis;

import com.bugull.redis.utils.Constant;
import com.bugull.redis.mq.MQClient;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Connection to redis server. 
 * <p>
 * Singleton Pattern is used here. Each application should have only one connection. 
 * There is a pool inside the connection. You can set the pool parameter if the default configuration doesn't suit your application.
 * </p>
 * 
 * <p>
 * You must do connect only once when application starts, and disconnect it when application exit.
 * </p>
 * 
 * @author Frank Wen(xbwen@hotmail.com)
 */
public class RedisConnection {
    
    private JedisPool pool;
    
    private MQClient mqClient;
    
    private JedisPoolConfig poolConfig = new JedisPoolConfig();
    private String host;
    private int port = Constant.DEFAULT_PORT;
    private int timeout = Constant.DEFAULT_TIMEOUT;
    private int database = Constant.DEFAULT_DATABASE;
    private String password;
    
    private RedisConnection(){
        
    }
    
    private static class Holder {
        final static RedisConnection instance = new RedisConnection();
    } 
    
    public static RedisConnection getInstance(){
        return Holder.instance;
    }
    
    public void connect(){
        poolConfig.setJmxEnabled(false);
        pool = new JedisPool(poolConfig, host, port, timeout, password, database);
    }
    
    public void disconnect(){
        if(mqClient != null){
            mqClient.stopAllConsume();
            mqClient.stopAllTopicTask();
        }
        if(pool != null){
            pool.destroy();
        }
    }
    
    public MQClient getMQClient(){
        if(mqClient == null){
            synchronized(this){
                if(mqClient == null){
                    mqClient = new MQClient();
                }
            }
        }
        return mqClient;
    }

    public void setPoolConfig(JedisPoolConfig poolConfig) {
        this.poolConfig = poolConfig;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public void setPassword(String password) {
        this.password = password;
    }
    
    public void setDatabase(int database){
        this.database = database;
    }

    public JedisPool getPool() {
        return pool;
    }

}
