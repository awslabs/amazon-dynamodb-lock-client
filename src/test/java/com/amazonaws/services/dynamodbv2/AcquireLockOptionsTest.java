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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.amazonaws.metrics.RequestMetricCollector;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.internal.InternalUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;

/**
 * Unit tests for AcquireLockOptions.
 *
 * @author <a href="mailto:amcp@amazon.com">Alexander Patrikalakis</a> 2017-07-13
 */
@RunWith(MockitoJUnitRunner.class)
public class AcquireLockOptionsTest {

    RequestMetricCollector metricCollector = RequestMetricCollector.NONE;

    @Test
    public void test_initializeOptionsWithRequestMetrics() {
        final AcquireLockOptions options = AcquireLockOptions.builder("hashKey").withRequestMetricCollector(this.metricCollector).build();
        assertEquals(this.metricCollector, options.getRequestMetricCollector().get());
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
            .withRequestMetricCollector(metricCollector).build();
        assertNotEquals(left, right);
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
            .withRequestMetricCollector(metricCollector).build();
        assertNotEquals(left, right);
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
            .withRequestMetricCollector(metricCollector).build();
        assertNotEquals(left, right);
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
                .withRequestMetricCollector(metricCollector).build();
        assertNotEquals(left, right);
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
            .withAcquireReleasedLocksConsistently(false)
            //.withSessionMonitor(1L, Optional.empty()) never equal if session monitor is set
            .withRequestMetricCollector(metricCollector).build();
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
                .withAcquireReleasedLocksConsistently(false)
                //.withSessionMonitor(1L, Optional.empty()) never equal if session monitor is set
                .withRequestMetricCollector(metricCollector).build();
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
            .withRequestMetricCollector(metricCollector).build();
        assertNotEquals(left, right);
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
            .withRequestMetricCollector(metricCollector).build();
        assertNotEquals(left, right);
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
            .withRequestMetricCollector(metricCollector).build();
        assertNotEquals(left, right);
    }

    @Test
    public void equals_additionalAttributesDifferent_returnsFalse() {
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
            .withAdditionalAttributes(InternalUtils.toAttributeValues(new Item().withNull("asdf")))
            .withUpdateExistingLockRecord(false)
            .withAcquireReleasedLocksConsistently(false)
            //.withSessionMonitor(1L, Optional.empty()) never equal if session monitor is set
            .withRequestMetricCollector(metricCollector).build();
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
                .withAcquireReleasedLocksConsistently(false)
                //.withSessionMonitor(1L, Optional.empty()) never equal if session monitor is set
                .withRequestMetricCollector(metricCollector).build();
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
                .withAcquireReleasedLocksConsistently(false)
                //.withSessionMonitor(1L, Optional.empty()) never equal if session monitor is set
                .withRequestMetricCollector(metricCollector).build();
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
                .withAcquireReleasedLocksConsistently(true)
                //.withSessionMonitor(1L, Optional.empty()) never equal if session monitor is set
                .withRequestMetricCollector(metricCollector).build();
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
            .withSessionMonitor(1L, Optional.empty()) //never equal if session monitor is set
            .withRequestMetricCollector(metricCollector).build();
        assertNotEquals(left, right);
    }

    @Test
    public void equals_metricCollectorDifferent_returnsFalse() {
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
            //.withSessionMonitor(1L, Optional.empty()) //never equal if session monitor is set
            .withRequestMetricCollector(null).build();
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
            .withAcquireReleasedLocksConsistently(false)
            //.withSessionMonitor(1L, Optional.empty()) never equal if session monitor is set
            .withRequestMetricCollector(metricCollector);

    }
}
