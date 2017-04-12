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
package com.amazonaws.services.dynamodbv2.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

/**
 * Unit tests for Exceptions defined in this class.
 *
 * @author <a href="mailto:amcp@amazon.co.jp">Alexander Patrikalakis</a> 2017-07-13
 */
public class ExceptionTests {
    @Test
    public void constructorWithMessageAndCause_SessionMonitorNotSetException() {
        final IllegalArgumentException iae = new IllegalArgumentException();
        SessionMonitorNotSetException e = new SessionMonitorNotSetException("message", iae);
        assertEquals(iae, e.getCause());
        assertEquals("message", e.getMessage());
    }
    @Test
    public void constructorNoArgs_LockNotGrantedException() {
        LockNotGrantedException e = new LockNotGrantedException();
        assertNull(e.getCause());
    }
}
