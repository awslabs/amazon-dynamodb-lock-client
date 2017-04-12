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
