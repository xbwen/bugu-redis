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

import redis.clients.jedis.Jedis;

/**
 * Store the blocked jedis client, in order to close it.
 * 
 * @author Frank Wen(xbwen@hotmail.com)
 */
public abstract class BlockedTask implements Runnable {
    
    protected Jedis jedis;
    protected boolean stopped;
    
    public Jedis getJedis(){
        return jedis;
    }
    
    public void setStopped(boolean stopped){
        this.stopped = stopped;
    }

}
