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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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

/**
 * Unit tests for AcquireLockOptions.
 *
 * @author <a href="mailto:amcp@amazon.co.jp">Alexander Patrikalakis</a> 2017-07-13
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
        assertTrue(left.equals(right));
    }

    @Test
    public void equals_partitionKeyDifferent_returnsFalse() {
        AcquireLockOptions left = createLockOptions();
        AcquireLockOptions right = AcquireLockOptions.builder("squat")
            .withSortKey("sortKey")
            .withData(ByteBuffer.wrap("data".getBytes()))
            .withReplaceData(true)
            .withDeleteLockOnRelease(true)
            .withRefreshPeriod(1l)
            .withAdditionalTimeToWaitForLock(1l)
            .withTimeUnit(TimeUnit.MILLISECONDS)
            .withAdditionalAttributes(new HashMap<>())
            //.withSessionMonitor(1L, Optional.empty()) never equal if session monitor is set
            .withRequestMetricCollector(metricCollector).build();
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
            .withRefreshPeriod(1l)
            .withAdditionalTimeToWaitForLock(1l)
            .withTimeUnit(TimeUnit.MILLISECONDS)
            .withAdditionalAttributes(new HashMap<>())
            //.withSessionMonitor(1L, Optional.empty()) never equal if session monitor is set
            .withRequestMetricCollector(metricCollector).build();
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
            .withRefreshPeriod(1l)
            .withAdditionalTimeToWaitForLock(1l)
            .withTimeUnit(TimeUnit.MILLISECONDS)
            .withAdditionalAttributes(new HashMap<>())
            //.withSessionMonitor(1L, Optional.empty()) never equal if session monitor is set
            .withRequestMetricCollector(metricCollector).build();
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
                .withRefreshPeriod(1l)
                .withAdditionalTimeToWaitForLock(1l)
                .withTimeUnit(TimeUnit.MILLISECONDS)
                .withAdditionalAttributes(new HashMap<>())
                //.withSessionMonitor(1L, Optional.empty()) never equal if session monitor is set
                .withRequestMetricCollector(metricCollector).build();
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
            .withRefreshPeriod(1l)
            .withAdditionalTimeToWaitForLock(1l)
            .withTimeUnit(TimeUnit.MILLISECONDS)
            .withAdditionalAttributes(new HashMap<>())
            //.withSessionMonitor(1L, Optional.empty()) never equal if session monitor is set
            .withRequestMetricCollector(metricCollector).build();
        assertFalse(left.equals(right));
    }

    @Test
    public void equals_refreshPeriodDifferent_returnsFalse() {
        AcquireLockOptions left = createLockOptions();
        AcquireLockOptions right = AcquireLockOptions.builder("partitionKey")
            .withSortKey("sortKey")
            .withData(ByteBuffer.wrap("data".getBytes()))
            .withReplaceData(true)
            .withDeleteLockOnRelease(true)
            .withRefreshPeriod(2l)
            .withAdditionalTimeToWaitForLock(1l)
            .withTimeUnit(TimeUnit.MILLISECONDS)
            .withAdditionalAttributes(new HashMap<>())
            //.withSessionMonitor(1L, Optional.empty()) never equal if session monitor is set
            .withRequestMetricCollector(metricCollector).build();
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
            .withRefreshPeriod(1l)
            .withAdditionalTimeToWaitForLock(2l)
            .withTimeUnit(TimeUnit.MILLISECONDS)
            .withAdditionalAttributes(new HashMap<>())
            //.withSessionMonitor(1L, Optional.empty()) never equal if session monitor is set
            .withRequestMetricCollector(metricCollector).build();
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
            .withRefreshPeriod(1l)
            .withAdditionalTimeToWaitForLock(1l)
            .withTimeUnit(TimeUnit.SECONDS)
            .withAdditionalAttributes(new HashMap<>())
            //.withSessionMonitor(1L, Optional.empty()) never equal if session monitor is set
            .withRequestMetricCollector(metricCollector).build();
        assertFalse(left.equals(right));
    }

    @Test
    public void equals_additionalAttributesDifferent_returnsFalse() {
        AcquireLockOptions left = createLockOptions();
        AcquireLockOptions right = AcquireLockOptions.builder("partitionKey")
            .withSortKey("sortKey")
            .withData(ByteBuffer.wrap("data".getBytes()))
            .withReplaceData(true)
            .withDeleteLockOnRelease(true)
            .withRefreshPeriod(1l)
            .withAdditionalTimeToWaitForLock(1l)
            .withTimeUnit(TimeUnit.MILLISECONDS)
            .withAdditionalAttributes(InternalUtils.toAttributeValues(new Item().withNull("asdf")))
            //.withSessionMonitor(1L, Optional.empty()) never equal if session monitor is set
            .withRequestMetricCollector(metricCollector).build();
        assertFalse(left.equals(right));
    }

    @Test
    public void equals_sessionMonitorSet_returnsFalse() {
        AcquireLockOptions left = createLockOptions();
        AcquireLockOptions right = AcquireLockOptions.builder("partitionKey")
            .withSortKey("sortKey")
            .withData(ByteBuffer.wrap("data".getBytes()))
            .withReplaceData(true)
            .withDeleteLockOnRelease(true)
            .withRefreshPeriod(1l)
            .withAdditionalTimeToWaitForLock(1l)
            .withTimeUnit(TimeUnit.MILLISECONDS)
            .withAdditionalAttributes(new HashMap<>())
            .withSessionMonitor(1L, Optional.empty()) //never equal if session monitor is set
            .withRequestMetricCollector(metricCollector).build();
        assertFalse(left.equals(right));
    }

    @Test
    public void equals_metricCollectorDifferent_returnsFalse() {
        AcquireLockOptions left = createLockOptions();
        AcquireLockOptions right = AcquireLockOptions.builder("partitionKey")
            .withSortKey("sortKey")
            .withData(ByteBuffer.wrap("data".getBytes()))
            .withReplaceData(true)
            .withDeleteLockOnRelease(true)
            .withRefreshPeriod(1l)
            .withAdditionalTimeToWaitForLock(1l)
            .withTimeUnit(TimeUnit.MILLISECONDS)
            .withAdditionalAttributes(new HashMap<>())
            //.withSessionMonitor(1L, Optional.empty()) //never equal if session monitor is set
            .withRequestMetricCollector(null).build();
        assertFalse(left.equals(right));
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
            .withRefreshPeriod(1l)
            .withAdditionalTimeToWaitForLock(1l)
            .withTimeUnit(TimeUnit.MILLISECONDS)
            .withAdditionalAttributes(new HashMap<>())
            //.withSessionMonitor(1L, Optional.empty()) never equal if session monitor is set
            .withRequestMetricCollector(metricCollector);
    }
}
