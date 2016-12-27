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

package com.bugull.redis.utils;

/**
 * Constants for bugu-redis framework.
 * 
 * @author Frank Wen(xbwen@hotmail.com)
 */
public final class Constant {
    
    //default value for connection
    public static final int DEFAULT_PORT = 6379;
    public static final int DEFAULT_TIMEOUT = 5000;  //in milliseconds
    public static final int DEFAULT_DATABASE = 0;
    
    //some timeout value
    public static final int SUBSCRIBE_TIMEOUT = 10;  //in seconds
    public static final int BLOCK_POP_TIMEOUT = 30; //in seconds
    
    public static final String MSG = "msg:";
    
    //for topic
    public static final String RETAIN = "retain:";
    
    //for idle
    public static final byte[] HEART_BEAT = {0x00};
    public static final int HEART_BEAT_TIME = 6;  //in seconds
    
}
