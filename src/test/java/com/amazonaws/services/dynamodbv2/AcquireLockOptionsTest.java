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
import static org.junit.Assert.assertNotNull;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Unit tests for AcquireLockOptions.
 *
 * @author <a href="mailto:amcp@amazon.com">Alexander Patrikalakis</a> 2017-07-13
 */
@RunWith(MockitoJUnitRunner.class)
public class AcquireLockOptionsTest {

    @Test
    public void test_initializeOptionsWithRequestMetrics() {
        final AcquireLockOptions options = AcquireLockOptions.builder("hashKey").build();
        assertNotNull(options);
    }

    @Test
    public void equals_leftOkRightNull_returnsFalse() {
        AcquireLockOptions left = createLockOptions();
        assertFalse(left.equals(null));
    }

    @Test
    public void equals_leftOkRightNotAcquireLockOptions_returnsFalse() {
        AcquireLockOptions left = createLockOptions();
        assertFalse(left.equals(""));
    }

    @Test
    public void equals_allSame_returnsTrue() {
        AcquireLockOptions left = createLockOptions();
        AcquireLockOptions right = createLockOptions();
        assertEquals(left, right);
    }

    @Test
    public void equals_partitionKeyDifferent_returnsFalse() {
        AcquireLockOptions left = createLockOptions();
        AcquireLockOptions right = AcquireLockOptions.builder("squat")
            .withSortKey("sortKey")
            .withData(ByteBuffer.wrap("data".getBytes()))
            .withReplaceData(true)
            .withDeleteLockOnRelease(true)
            .withAcquireOnlyIfLockAlreadyExists(false)
            .withRefreshPeriod(1l)
            .withAdditionalTimeToWaitForLock(1l)
            .withTimeUnit(TimeUnit.MILLISECONDS)
            .withAdditionalAttributes(new HashMap<>())
            .withUpdateExistingLockRecord(false)
            .withAcquireReleasedLocksConsistently(false)
            //.withSessionMonitor(1L, Optional.empty()) never equal if session monitor is set
            .build();
        assertFalse(left.equals(right));
    }

    @Test
    public void equals_sortKeyDifferent_returnsFalse() {
        AcquireLockOptions left = createLockOptions();
        AcquireLockOptions right = AcquireLockOptions.builder("partitionKey")
            .withSortKey("squat")
            .withData(ByteBuffer.wrap("data".getBytes()))
            .withReplaceData(true)
            .withDeleteLockOnRelease(true)
            .withAcquireOnlyIfLockAlreadyExists(false)
            .withRefreshPeriod(1l)
            .withAdditionalTimeToWaitForLock(1l)
            .withTimeUnit(TimeUnit.MILLISECONDS)
            .withAdditionalAttributes(new HashMap<>())
            .withUpdateExistingLockRecord(false)
            .withAcquireReleasedLocksConsistently(false)
            //.withSessionMonitor(1L, Optional.empty()) never equal if session monitor is set
            .build();
        assertFalse(left.equals(right));
    }

    @Test
    public void equals_dataDifferent_returnsFalse() {
        AcquireLockOptions left = createLockOptions();
        AcquireLockOptions right = AcquireLockOptions.builder("partitionKey")
            .withSortKey("sortKey")
            .withData(ByteBuffer.wrap("squat".getBytes()))
            .withReplaceData(true)
            .withDeleteLockOnRelease(true)
            .withAcquireOnlyIfLockAlreadyExists(false)
            .withRefreshPeriod(1l)
            .withAdditionalTimeToWaitForLock(1l)
            .withTimeUnit(TimeUnit.MILLISECONDS)
            .withAdditionalAttributes(new HashMap<>())
            .withUpdateExistingLockRecord(false)
            .withAcquireReleasedLocksConsistently(false)
            .withShouldSkipBlockingWait(true)
            //.withSessionMonitor(1L, Optional.empty()) never equal if session monitor is set
            .build();
        assertFalse(left.equals(right));
    }

    @Test
    public void equals_replaceDataDifferent_returnsFalse() {
        AcquireLockOptions left = createLockOptions();
        AcquireLockOptions right =
            AcquireLockOptions.builder("partitionKey")
                .withSortKey("sortKey")
                .withData(ByteBuffer.wrap("data".getBytes()))
                .withReplaceData(false)
                .withDeleteLockOnRelease(true)
                .withAcquireOnlyIfLockAlreadyExists(false)
                .withRefreshPeriod(1l)
                .withAdditionalTimeToWaitForLock(1l)
                .withTimeUnit(TimeUnit.MILLISECONDS)
                .withAdditionalAttributes(new HashMap<>())
                .withUpdateExistingLockRecord(false)
                .withAcquireReleasedLocksConsistently(false)
                //.withSessionMonitor(1L, Optional.empty()) never equal if session monitor is set
                .build();
        assertFalse(left.equals(right));
    }

    @Test
    public void equals_deleteLockOnReleaseDifferent_returnsFalse() {
        AcquireLockOptions left = createLockOptions();
        AcquireLockOptions right = AcquireLockOptions.builder("partitionKey")
            .withSortKey("sortKey")
            .withData(ByteBuffer.wrap("data".getBytes()))
            .withReplaceData(true)
            .withDeleteLockOnRelease(false)
            .withAcquireOnlyIfLockAlreadyExists(false)
            .withRefreshPeriod(1l)
            .withAdditionalTimeToWaitForLock(1l)
            .withTimeUnit(TimeUnit.MILLISECONDS)
            .withAdditionalAttributes(new HashMap<>())
            .withUpdateExistingLockRecord(false)
            .withAcquireReleasedLocksConsistently(false).build();
            //.withSessionMonitor(1L, Optional.empty()) never equal if session monitor is set
        assertNotEquals(left, right);
    }

    @Test
    public void equals_acquireOnlyIfLockAlreadyExistsDifferent_returnsFalse() {
        AcquireLockOptions left = createLockOptions();
        AcquireLockOptions right = AcquireLockOptions.builder("partitionKey")
                .withSortKey("sortKey")
                .withData(ByteBuffer.wrap("data".getBytes()))
                .withReplaceData(true)
                .withDeleteLockOnRelease(true)
                .withAcquireOnlyIfLockAlreadyExists(true)
                .withRefreshPeriod(1l)
                .withAdditionalTimeToWaitForLock(1l)
                .withTimeUnit(TimeUnit.MILLISECONDS)
                .withAdditionalAttributes(new HashMap<>())
                .withUpdateExistingLockRecord(false)
                .withAcquireReleasedLocksConsistently(false).build();
                //.withSessionMonitor(1L, Optional.empty()) never equal if session monitor is set
        assertNotEquals(left, right);
    }

    @Test
    public void equals_refreshPeriodDifferent_returnsFalse() {
        AcquireLockOptions left = createLockOptions();
        AcquireLockOptions right = AcquireLockOptions.builder("partitionKey")
            .withSortKey("sortKey")
            .withData(ByteBuffer.wrap("data".getBytes()))
            .withReplaceData(true)
            .withDeleteLockOnRelease(true)
            .withAcquireOnlyIfLockAlreadyExists(false)
            .withRefreshPeriod(2l)
            .withAdditionalTimeToWaitForLock(1l)
            .withTimeUnit(TimeUnit.MILLISECONDS)
            .withAdditionalAttributes(new HashMap<>())
            .withUpdateExistingLockRecord(false)
            .withAcquireReleasedLocksConsistently(false)
            //.withSessionMonitor(1L, Optional.empty()) never equal if session monitor is set
            .build();
        assertFalse(left.equals(right));
    }

    @Test
    public void equals_additionalTimeToWaitForLockDifferent_returnsFalse() {
        AcquireLockOptions left = createLockOptions();
        AcquireLockOptions right = AcquireLockOptions.builder("partitionKey")
            .withSortKey("sortKey")
            .withData(ByteBuffer.wrap("data".getBytes()))
            .withReplaceData(true)
            .withDeleteLockOnRelease(true)
            .withAcquireOnlyIfLockAlreadyExists(false)
            .withRefreshPeriod(1l)
            .withAdditionalTimeToWaitForLock(2l)
            .withTimeUnit(TimeUnit.MILLISECONDS)
            .withAdditionalAttributes(new HashMap<>())
            .withUpdateExistingLockRecord(false)
            .withAcquireReleasedLocksConsistently(false)
            //.withSessionMonitor(1L, Optional.empty()) never equal if session monitor is set
            .build();
        assertFalse(left.equals(right));
    }

    @Test
    public void equals_timeUnitDifferent_returnsFalse() {
        AcquireLockOptions left = createLockOptions();
        AcquireLockOptions right = AcquireLockOptions.builder("partitionKey")
            .withSortKey("sortKey")
            .withData(ByteBuffer.wrap("data".getBytes()))
            .withReplaceData(true)
            .withDeleteLockOnRelease(true)
            .withAcquireOnlyIfLockAlreadyExists(false)
            .withRefreshPeriod(1l)
            .withAdditionalTimeToWaitForLock(1l)
            .withTimeUnit(TimeUnit.SECONDS)
            .withAdditionalAttributes(new HashMap<>())
            .withUpdateExistingLockRecord(false)
            .withAcquireReleasedLocksConsistently(false)
            //.withSessionMonitor(1L, Optional.empty()) never equal if session monitor is set
            .build();
        assertFalse(left.equals(right));
    }

    @Test
    public void equals_additionalAttributesDifferent_returnsFalse() {
        AcquireLockOptions left = createLockOptions();
        Map<String, AttributeValue> additionalAttributes = new HashMap<>();
        additionalAttributes.put("asdf", AttributeValue.builder().nul(Boolean.TRUE).build());
        AcquireLockOptions right = AcquireLockOptions.builder("partitionKey")
            .withSortKey("sortKey")
            .withData(ByteBuffer.wrap("data".getBytes()))
            .withReplaceData(true)
            .withDeleteLockOnRelease(true)
            .withAcquireOnlyIfLockAlreadyExists(false)
            .withRefreshPeriod(1l)
            .withAdditionalTimeToWaitForLock(1l)
            .withTimeUnit(TimeUnit.MILLISECONDS)
            .withAdditionalAttributes(additionalAttributes)
            .withUpdateExistingLockRecord(false)
            .withAcquireReleasedLocksConsistently(false).build();
            //.withSessionMonitor(1L, Optional.empty()) never equal if session monitor is set
        assertNotEquals(left, right);
    }

    @Test
    public void equals_updateExistingLockRecordDifferent_returnsFalse() {
        AcquireLockOptions left = createLockOptions();
        AcquireLockOptions right = AcquireLockOptions.builder("partitionKey")
                .withSortKey("sortKey")
                .withData(ByteBuffer.wrap("data".getBytes()))
                .withReplaceData(true)
                .withDeleteLockOnRelease(true)
                .withAcquireOnlyIfLockAlreadyExists(false)
                .withRefreshPeriod(1l)
                .withAdditionalTimeToWaitForLock(1l)
                .withTimeUnit(TimeUnit.MILLISECONDS)
                .withAdditionalAttributes(new HashMap<>())
                .withUpdateExistingLockRecord(true)
                .withAcquireReleasedLocksConsistently(false).build();
                //.withSessionMonitor(1L, Optional.empty()) never equal if session monitor is set
        assertNotEquals(left, right);
    }

    @Test
    public void equals_shouldSkipBlockingWaitDifferent_returnsFalse() {
        AcquireLockOptions left = createLockOptions();
        AcquireLockOptions right = AcquireLockOptions.builder("partitionKey")
                .withSortKey("sortKey")
                .withData(ByteBuffer.wrap("data".getBytes()))
                .withReplaceData(true)
                .withDeleteLockOnRelease(true)
                .withAcquireOnlyIfLockAlreadyExists(false)
                .withRefreshPeriod(1l)
                .withAdditionalTimeToWaitForLock(1l)
                .withTimeUnit(TimeUnit.MILLISECONDS)
                .withAdditionalAttributes(new HashMap<>())
                .withUpdateExistingLockRecord(false)
                .withShouldSkipBlockingWait(false)
                .withAcquireReleasedLocksConsistently(false).build();
                //.withSessionMonitor(1L, Optional.empty()) never equal if session monitor is set
        assertNotEquals(left, right);
    }

    @Test
    public void equals_consistentLockDataDifferent_returnsFalse() {
        AcquireLockOptions left = createLockOptions();
        AcquireLockOptions right = AcquireLockOptions.builder("partitionKey")
                .withSortKey("sortKey")
                .withData(ByteBuffer.wrap("data".getBytes()))
                .withReplaceData(true)
                .withDeleteLockOnRelease(true)
                .withAcquireOnlyIfLockAlreadyExists(false)
                .withRefreshPeriod(1l)
                .withAdditionalTimeToWaitForLock(1l)
                .withTimeUnit(TimeUnit.MILLISECONDS)
                .withAdditionalAttributes(new HashMap<>())
                .withUpdateExistingLockRecord(false)
                .withAcquireReleasedLocksConsistently(true).build();
                //.withSessionMonitor(1L, Optional.empty()) never equal if session monitor is set
        assertNotEquals(left, right);
    }

    @Test
    public void equals_sessionMonitorSet_returnsFalse() {
        AcquireLockOptions left = createLockOptions();
        AcquireLockOptions right = AcquireLockOptions.builder("partitionKey")
            .withSortKey("sortKey")
            .withData(ByteBuffer.wrap("data".getBytes()))
            .withReplaceData(true)
            .withDeleteLockOnRelease(true)
            .withAcquireOnlyIfLockAlreadyExists(false)
            .withRefreshPeriod(1l)
            .withAdditionalTimeToWaitForLock(1l)
            .withTimeUnit(TimeUnit.MILLISECONDS)
            .withAdditionalAttributes(new HashMap<>())
            .withUpdateExistingLockRecord(false)
            .withAcquireReleasedLocksConsistently(false)
            .withSessionMonitor(1L, Optional.empty()).build(); //never equal if session monitor is set
        assertNotEquals(left, right);
    }

    @Test
    public void hashCode_doesntThrow() {
        createLockOptionsBuilder().hashCode();
        createLockOptions().hashCode();
    }

    @Test
    public void toString_doesntThrow() {
        createLockOptionsBuilder().toString();
        createLockOptions().toString();
    }

    private AcquireLockOptions createLockOptions() {
        return createLockOptionsBuilder().build();
    }

    private AcquireLockOptions.AcquireLockOptionsBuilder createLockOptionsBuilder() {
        return AcquireLockOptions.builder("partitionKey")
            .withSortKey("sortKey")
            .withData(ByteBuffer.wrap("data".getBytes()))
            .withReplaceData(true)
            .withDeleteLockOnRelease(true)
            .withAcquireOnlyIfLockAlreadyExists(false)
            .withRefreshPeriod(1l)
            .withAdditionalTimeToWaitForLock(1l)
            .withTimeUnit(TimeUnit.MILLISECONDS)
            .withAdditionalAttributes(new HashMap<>())
            .withUpdateExistingLockRecord(false)
            .withShouldSkipBlockingWait(true)
            .withAcquireReleasedLocksConsistently(false);
            //.withSessionMonitor(1L, Optional.empty()) never equal if session monitor is set

    }
}
