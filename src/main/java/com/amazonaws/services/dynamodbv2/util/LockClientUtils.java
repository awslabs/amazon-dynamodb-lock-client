/**
 * Copyright 2013-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * <p>
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 * <p>
 * http://aws.amazon.com/asl/
 * <p>
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express
 * or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */
package com.amazonaws.services.dynamodbv2.util;


/**
 * A class containing static utility functions. These functions are used throughout the lock client codebase.
 *
 * @author <a href="mailto:slutsker@amazon.com">Sasha Slutsker</a>
 * @author <a href="mailto:amcp@amazon.co.jp">Alexander Patrikalakis</a>
 */
public enum LockClientUtils {
    INSTANCE;
    /**
     * Calls System.nanoTime() and converts it to milliseconds. This is used by the lock client, since the lock client
     * uses milliseconds as its base unit instead of nanoseconds.
     *
     * @return the current time in milliseconds, but not since epoch. It is only useful for elapsed time, not absolute time.
     */
    public long millisecondTime() {
        return System.nanoTime() / 1000000;
    }
}
