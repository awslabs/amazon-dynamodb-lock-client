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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Unit tests for LockClientUtils.
 *
 * @author <a href="mailto:amcp@amazon.co.jp">Alexander Patrikalakis</a> 2017-07-13
 */
public class LockClientUtilsTests {
    @Test
    public void values_returnsSingletonCollectionWithInstance() {
        LockClientUtils[] values = { LockClientUtils.INSTANCE };
        assertArrayEquals(values, LockClientUtils.values());
    }
    @Test
    public void valueOf_whenValueIsInstance_returnsInstance() {
        assertEquals(LockClientUtils.INSTANCE, LockClientUtils.valueOf("INSTANCE"));
    }
}
