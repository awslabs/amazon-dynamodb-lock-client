/**
 * Copyright 2013-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;

import com.amazonaws.services.dynamodbv2.model.LockNotGrantedException;
import com.amazonaws.services.dynamodbv2.model.SessionMonitorNotSetException;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * LockItem unit tests.
 *
 * @author <a href="mailto:amcp@amazon.com">Alexander Patrikalakis</a> 2017-07-13
 */
@RunWith(MockitoJUnitRunner.class)
public class LockItemTest {
    @Mock
    AmazonDynamoDBLockClient lockClient;
    @Test
    public void hashCode_returns() {
        createLockItem(lockClient).hashCode();
    }

    LockItem mockableLockItem = Mockito.spy(createLockItem(lockClient));
    @Test
    public void equals_rightNull_returnFalse() {
        assertFalse(createLockItem(lockClient).equals(null));
    }

    @Test
    public void equals_rightNotLockItem_returnFalse() {
        assertFalse(createLockItem(lockClient).equals(""));
    }

    @Test
    public void equals_differentPartitionKey_returnFalse() {
        assertFalse(createLockItem(lockClient).equals(new LockItem(lockClient, "squat",
            Optional.of("sortKey"),
            Optional.of(ByteBuffer.wrap("data".getBytes())),
            false, //delete lock item on close
            "ownerName",
            1L, //lease duration
            1000, //last updated time in milliseconds
            "recordVersionNumber",
            false, //released
            Optional.of(new SessionMonitor(1000, Optional.empty(), Thread::new)), //session monitor
            new HashMap<>())));
    }

    @Test
    public void equals_differentOwner_returnFalse() {
        assertFalse(createLockItem(lockClient).equals(new LockItem(lockClient, "partitionKey",
            Optional.of("sortKey"),
            Optional.of(ByteBuffer.wrap("data".getBytes())),
            false, //delete lock item on close
            "squat",
            1L, //lease duration
            1000, //last updated time in milliseconds
            "recordVersionNumber",
            false, //released
            Optional.of(new SessionMonitor(1000, Optional.empty(), Thread::new)), //session monitor
            new HashMap<>())));
    }

    @Test
    public void equals_same_returnTrue() {
        assertTrue(createLockItem(lockClient).equals(createLockItem(lockClient)));
    }

    @Test
    public void isExpired_whenIsReleasedTrue_returnTrue() {
        assertTrue(new LockItem(lockClient, "partitionKey",
            Optional.of("sortKey"),
            Optional.of(ByteBuffer.wrap("data".getBytes())),
            false, //delete lock item on close
            "ownerName",
            1L, //lease duration
            1000, //last updated time in milliseconds
            "recordVersionNumber",
            true, //released
            Optional.of(new SessionMonitor(1000, Optional.empty(), Thread::new)), //session monitor
            new HashMap<>()).isExpired());
    }

    @Test
    public void isReleased_whenIsReleasedFalseInConstructor_returnsFalse() {
        assertFalse(createLockItem(lockClient).isReleased());
    }

    @Test(expected = LockNotGrantedException.class)
    public void ensure_whenIsReleasedTrue_throwsLockNotGrantedException() {
        new LockItem(lockClient, "partitionKey",
            Optional.of("sortKey"),
            Optional.of(ByteBuffer.wrap("data".getBytes())),
            false, //delete lock item on close
            "ownerName",
            1L, //lease duration
            1000, //last updated time in milliseconds
            "recordVersionNumber",
            true, //released
            Optional.empty(), //session monitor
            new HashMap<>()).ensure(2L, TimeUnit.MILLISECONDS);
    }

    @Test(expected = IllegalStateException.class)
    public void millisecondsUntilDangerZoneEntered_whenIsReleasedTrue_throwsIllegalStateException() {
        new LockItem(lockClient, "partitionKey",
            Optional.of("sortKey"),
            Optional.of(ByteBuffer.wrap("data".getBytes())),
            false, //delete lock item on close
            "ownerName",
            1L, //lease duration
            1000, //last updated time in milliseconds
            "recordVersionNumber",
            true, //released
            Optional.of(new SessionMonitor(1000, Optional.empty(), Thread::new)), //session monitor
            new HashMap<>()).millisecondsUntilDangerZoneEntered();
    }

    @Test
    public void amIAboutToExpire_whenMsUntilDangerZoneEnteredZero_returnsTrue() {
        when(mockableLockItem.millisecondsUntilDangerZoneEntered()).thenReturn(0L);
        assertTrue(mockableLockItem.amIAboutToExpire());
    }

    @Test
    public void amIAboutToExpire_whenMsUntilDangerZoneEnteredOne_returnsFalse() {
        when(mockableLockItem.millisecondsUntilDangerZoneEntered()).thenReturn(1L);
        assertFalse(mockableLockItem.amIAboutToExpire());
    }

    @Test(expected = SessionMonitorNotSetException.class)
    public void hasCallback_sessionMonitorNotPresent_throwSessionMonitorNotSetException() {
        new LockItem(lockClient, "partitionKey",
            Optional.of("sortKey"),
            Optional.of(ByteBuffer.wrap("data".getBytes())),
            false, //delete lock item on close
            "ownerName",
            1L, //lease duration
            1000, //last updated time in milliseconds
            "recordVersionNumber",
            false, //released
            Optional.empty(), //session monitor
            new HashMap<>()).hasCallback();
    }

    @Test
    public void updateLookUpTime_whenLookUpTimeIsUpdated_thenGetLookUpTimeReturnsTheUpdatedTime() {
        LockItem lockItem = createLockItem(lockClient);
        assertEquals(1000, lockItem.getLookupTime());

        //update the look up time
        lockItem.updateLookUpTime(2000);

        assertEquals(2000, lockItem.getLookupTime());
    }

    static LockItem createLockItem(AmazonDynamoDBLockClient lockClient) {
        return new LockItem(lockClient, "partitionKey",
            Optional.of("sortKey"),
            Optional.of(ByteBuffer.wrap("data".getBytes())),
            false, //delete lock item on close
            "ownerName",
            1L, //lease duration
            1000, //last updated time in milliseconds
            "recordVersionNumber",
            false, //released
            Optional.of(new SessionMonitor(1000, Optional.empty(), Thread::new)), //session monitor
            new HashMap<>()); //additional attributes
    }
}
