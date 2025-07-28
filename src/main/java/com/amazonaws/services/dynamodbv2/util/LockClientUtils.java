/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
