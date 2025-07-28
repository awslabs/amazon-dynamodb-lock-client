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
package com.amazonaws.services.dynamodbv2.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

/**
 * Unit tests for Exceptions defined in this class.
 *
 * @author <a href="mailto:amcp@amazon.com">Alexander Patrikalakis</a> 2017-07-13
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

    @Test
    public void constructor_LockCurrentlyUnavailableException() {
        LockCurrentlyUnavailableException e = new LockCurrentlyUnavailableException();
        assertNull(e.getCause());
    }

    @Test
    public void constructorWithMessageAndCause_LockCurrentlyUnavailableException() {
        final IllegalArgumentException iae = new IllegalArgumentException();
        LockCurrentlyUnavailableException e = new LockCurrentlyUnavailableException("message", iae);
        assertEquals(iae, e.getCause());
        assertEquals("message", e.getMessage());
    }

    @Test
    public void constructorWithCause_LockCurrentlyUnavailableException() {
        final IllegalArgumentException iae = new IllegalArgumentException();
        LockCurrentlyUnavailableException e = new LockCurrentlyUnavailableException(iae);
        assertEquals(iae, e.getCause());
    }

    @Test
    public void constructorWithMessage_LockCurrentlyUnavailableException() {
        final IllegalArgumentException iae = new IllegalArgumentException();
        LockCurrentlyUnavailableException e = new LockCurrentlyUnavailableException("message");
        assertEquals("message", e.getMessage());
    }

    @Test
    public void constructorWithMessageAndCauseAndSuppressionAndStackTrace_LockCurrentlyUnavailableException() {
        final IllegalArgumentException iae = new IllegalArgumentException();
        LockCurrentlyUnavailableException e = new LockCurrentlyUnavailableException("message", iae, false, false);
        assertEquals(iae, e.getCause());
        assertEquals("message", e.getMessage());
    }
}
