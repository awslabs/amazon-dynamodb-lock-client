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
package com.amazonaws.services.dynamodbv2;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.when;

import java.util.Optional;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * SendHeartbeatOptions unit tests.
 *
 * @author <a href="mailto:amcp@amazon.co.jp">Alexander Patrikalakis</a> 2017-07-13
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({SessionMonitor.class})
public class SessionMonitorTest {
    @Test
    public void isLeaseEnteringDangerZone_whenThereAreZeroOrLessMillisUntilEnterDangerZone_returnTrue() {
        SessionMonitor sut = PowerMockito.spy(new SessionMonitor(1000, Optional.empty(), Thread::new));
        when(sut.millisecondsUntilLeaseEntersDangerZone(25L)).thenReturn(0L);
        assertTrue(sut.isLeaseEnteringDangerZone(25L));
    }

    @Test
    public void isLeaseEnteringDangerZone_whenThereAreMoreThanZeroMillisUntilEnterDangerZone_returnFalse() {
        SessionMonitor sut = PowerMockito.spy(new SessionMonitor(1000, Optional.empty(), Thread::new));
        when(sut.millisecondsUntilLeaseEntersDangerZone(25L)).thenReturn(1L);
        assertFalse(sut.isLeaseEnteringDangerZone(25L));
    }

    @Test
    public void runCallback_whenNotPresent_doesNothing() {
        SessionMonitor sut = new SessionMonitor(1000, Optional.empty(), Thread::new);
        sut.runCallback();
    }

    @Test
    public void hasCallback_whenCallbackNull_returnFalse() {
        SessionMonitor sut = new SessionMonitor(1000, null, Thread::new);
        assertFalse(sut.hasCallback());
    }

    @Test
    public void hasCallback_whenCallbackNotNull_returnTrue() {
        SessionMonitor sut = new SessionMonitor(1000, Optional.empty(), Thread::new);
        assertTrue(sut.hasCallback());
    }
}
