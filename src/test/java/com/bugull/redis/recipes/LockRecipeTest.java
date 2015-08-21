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

package com.bugull.redis.recipes;

import com.bugull.redis.RedisConnection;
import org.junit.Test;

/**
 *
 * @author Frank Wen(xbwen@hotmail.com)
 */
public class LockRecipeTest {
    
    @Test
    public void test() throws Exception {
        RedisConnection conn = RedisConnection.getInstance();
        conn.setHost("127.0.0.1");
        conn.setPassword("foobared");
        conn.connect();
        
        for(int i=0; i<10; i++){
            Runnable task = new GetResourceTask(i);
            new Thread(task).start();
        }
        
        Thread.sleep(30 * 1000);
        
        conn.disconnect();
    }
    
    class GetResourceTask implements Runnable {
        
        private int index;
        
        public GetResourceTask(int index){
            this.index = index;
        }
        @Override
        public void run() {
            try{
                LockRecipe lock = new LockRecipe();
                String lockValue = lock.acquireLock("the_lock");
                if(lockValue != null){
                    System.out.println("thread " + index + " acquired the lock");
                }else{
                    System.out.println("thread " + index + " acquire the lock failed!");
                }
                Thread.sleep(100);  //thread do something here to use the resource
                if(lockValue != null){
                    boolean success = lock.releaseLock("the_lock", lockValue);
                    if(success){
                        System.out.println("thread " + index + " released the lock");
                    }else{
                        System.out.println("thread " + index + " release the lock failed!");
                    }
                }
            }catch(Exception ex){
                
            }
        }
    }

}
