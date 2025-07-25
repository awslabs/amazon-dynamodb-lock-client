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
        SessionMonitor sut = PowerMockito.spy(new SessionMonitor(1000, Optional.empty()));
        when(sut.millisecondsUntilLeaseEntersDangerZone(25L)).thenReturn(0L);
        assertTrue(sut.isLeaseEnteringDangerZone(25L));
    }

    @Test
    public void isLeaseEnteringDangerZone_whenThereAreMoreThanZeroMillisUntilEnterDangerZone_returnFalse() {
        SessionMonitor sut = PowerMockito.spy(new SessionMonitor(1000, Optional.empty()));
        when(sut.millisecondsUntilLeaseEntersDangerZone(25L)).thenReturn(1L);
        assertFalse(sut.isLeaseEnteringDangerZone(25L));
    }

    @Test
    public void runCallback_whenNotPresent_doesNothing() {
        SessionMonitor sut = new SessionMonitor(1000, Optional.empty());
        sut.runCallback();
    }

    @Test
    public void hasCallback_whenCallbackNull_returnFalse() {
        SessionMonitor sut = new SessionMonitor(1000, null);
        assertFalse(sut.hasCallback());
    }

    @Test
    public void hasCallback_whenCallbackNotNull_returnTrue() {
        SessionMonitor sut = new SessionMonitor(1000, Optional.empty());
        assertTrue(sut.hasCallback());
    }
}
