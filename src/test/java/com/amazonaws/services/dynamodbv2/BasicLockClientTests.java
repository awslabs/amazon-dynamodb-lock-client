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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.amazonaws.AmazonClientException;
import com.amazonaws.metrics.RequestMetricCollector;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.LockNotGrantedException;
import com.amazonaws.services.dynamodbv2.model.LockTableDoesNotExistException;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.SessionMonitorNotSetException;
import com.amazonaws.services.dynamodbv2.util.LockClientUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Integration tests for the AmazonDynamoDBLockClient. These unit tests use a local version of DynamoDB.
 *
 * @author <a href="mailto:slutsker@amazon.com">Sasha Slutsker</a>
 * @author <a href="mailto:amcp@amazon.co.jp">Alexander Patrikalakis</a>
 */
public class BasicLockClientTests extends InMemoryLockClientTester {

    private static final String INTEGRATION_TESTER_2 = "integrationTester2";
    private static final String INTEGRATION_TESTER_3 = "integrationTester3";

    @Test
    public void testAcquireBasicLock() throws LockNotGrantedException, InterruptedException {
        this.lockClient.acquireLock(AcquireLockOptions.builder("testKey1").withData(ByteBuffer.wrap(TEST_DATA.getBytes())).build());
        assertTrue(this.lockClient.getLock("testKey1", Optional.empty()).get().getOwnerName().equals(INTEGRATION_TESTER));
    }

    @Test
    public void testAcquireBasicLockWithSortKey() throws LockNotGrantedException, InterruptedException {
        this.lockClient.acquireLock(AcquireLockOptions.builder("testKey1").withSortKey("1").withData(ByteBuffer.wrap(TEST_DATA.getBytes())).build());
        assertTrue(this.lockClient.getLock("testKey1", Optional.of("1")).get().getOwnerName().equals(INTEGRATION_TESTER));
    }

    /**
     * If the lock table was previously created, the client should correctly report that it exists.
     */
    @Test
    public void testLockTableExists_created() {
        assertTrue("Lock table should exist after it has been created", this.lockClient.lockTableExists());
    }

    /**
     * If the lock table was previously created, the existence assertion should succeed.
     */
    @Test
    public void testAssertLockTableExists_created() {
        this.lockClient.assertLockTableExists();
    }

    /**
     * If the lock table was not previously created, the client should correctly report that it doesn't exist.
     */
    @Test
    public void testLockTableExists_notCreated() {
        assertFalse("Lock table should not exist if it has not been created", this.lockClientNoTable.lockTableExists());
    }

    /**
     * If the lock table was not previously created, the existence assertion should fail.
     */
    @Test(expected = LockTableDoesNotExistException.class)
    public void testAssertLockTableExists_notCreated() {
        this.lockClientNoTable.assertLockTableExists();
    }

    /**
     * With a background thread, the lock does not expire
     *
     * @throws InterruptedException if acquireLock was interrupted
     * @throws IOException          in case we were unable to close the second lock client
     */
    @Test
    public void testBackgroundThread() throws InterruptedException {
        final LockItem item = lockClientWithHeartbeating.acquireLock(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1);
        assertEquals(item.getPartitionKey(), "testKey1");
        assertEquals(Optional.empty(), lockClientWithHeartbeating.tryAcquireLock(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1));
        final Optional<LockItem> item2 = lockClientWithHeartbeating.tryAcquireLock(ACQUIRE_LOCK_OPTIONS_TEST_KEY_2);
        assertNotEquals(Optional.empty(), item2);
        Thread.sleep(5000);
        assertEquals(Optional.empty(), lockClientWithHeartbeating.tryAcquireLock(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1));
        assertEquals(Optional.empty(), lockClientWithHeartbeating.tryAcquireLock(ACQUIRE_LOCK_OPTIONS_TEST_KEY_2));
        item.close();
        item2.get().close();
    }

    @Test
    public void testBackgroundThreadWithSortKey() throws InterruptedException {
        final LockItem item = lockClientWithHeartbeatingForRangeKeyTable.acquireLock(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1_SORT_1);
        assertEquals(item.getPartitionKey(), "testKey1");
        assertEquals(item.getSortKey(), Optional.of("1"));
        assertEquals(Optional.empty(), lockClientWithHeartbeatingForRangeKeyTable.tryAcquireLock(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1_SORT_1));
        final Optional<LockItem> item2 = lockClientWithHeartbeatingForRangeKeyTable.tryAcquireLock(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1_SORT_2);
        assertNotEquals(Optional.empty(), item2);
        assertEquals(item2.get().getPartitionKey(), "testKey1");
        assertEquals(item2.get().getSortKey(), Optional.of("2"));
        Thread.sleep(5000);
        assertEquals(Optional.empty(), lockClientWithHeartbeatingForRangeKeyTable.tryAcquireLock(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1_SORT_1));
        assertEquals(Optional.empty(), lockClientWithHeartbeatingForRangeKeyTable.tryAcquireLock(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1_SORT_2));
        item.close();
        item2.get().close();
    }

    /**
     * Without a background thread, the lock does expire
     *
     * @throws InterruptedException if acquireLock was interrupted
     */
    @Test
    public void testNoBackgroundThread() throws InterruptedException, IOException {
        final LockItem item = shortLeaseLockClient.acquireLock(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1);
        final LockItem item2 = shortLeaseLockClient.acquireLock(ACQUIRE_LOCK_OPTIONS_TEST_KEY_2);
        assertEquals(item.getPartitionKey(), "testKey1");
        assertEquals(item2.getPartitionKey(), "testKey2");
        Thread.sleep(5000);
        assertNotEquals(Optional.empty(), shortLeaseLockClient.tryAcquireLock(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1));
        assertNotEquals(Optional.empty(), shortLeaseLockClient.tryAcquireLock(ACQUIRE_LOCK_OPTIONS_TEST_KEY_2));
    }

    @Test
    public void testNoBackgroundThreadWithSortKey() throws InterruptedException, IOException {
        final LockItem item = shortLeaseLockClientForRangeKeyTable.acquireLock(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1_SORT_1);
        final LockItem item2 = shortLeaseLockClientForRangeKeyTable.acquireLock(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1_SORT_2);
        assertEquals(item.getPartitionKey(), "testKey1");
        assertEquals(item.getSortKey(), Optional.of("1"));
        assertEquals(item2.getPartitionKey(), "testKey1");
        assertEquals(item2.getSortKey(), Optional.of("2"));
        Thread.sleep(5000);
        assertNotEquals(Optional.empty(), shortLeaseLockClientForRangeKeyTable.tryAcquireLock(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1_SORT_1));
        assertNotEquals(Optional.empty(), shortLeaseLockClientForRangeKeyTable.tryAcquireLock(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1_SORT_2));
    }

    @Test
    public void testReleasingLock() throws IOException, LockNotGrantedException, InterruptedException {
        final LockItem item = this.lockClientWithHeartbeating.acquireLock(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1);
        assertEquals(item.getPartitionKey(), "testKey1");

        assertEquals(Optional.empty(), this.lockClientWithHeartbeating.tryAcquireLock(ACQUIRE_LOCK_OPTIONS_WITH_NO_WAIT));
        item.close();
        assertNotEquals(Optional.empty(), this.lockClientWithHeartbeating.tryAcquireLock(ACQUIRE_LOCK_OPTIONS_WITH_NO_WAIT));
    }

    @Test
    public void testReleasingLockWithSortKey() throws IOException, LockNotGrantedException, InterruptedException {
        final LockItem item = this.lockClientWithHeartbeatingForRangeKeyTable.acquireLock(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1_SORT_1);
        assertEquals(item.getPartitionKey(), "testKey1");
        assertEquals(item.getSortKey(), Optional.of("1"));
        assertEquals(Optional.empty(), this.lockClientWithHeartbeatingForRangeKeyTable.tryAcquireLock(ACQUIRE_LOCK_OPTIONS_WITH_NO_WAIT_SORT_1));
        item.close();
        assertNotEquals(Optional.empty(), this.lockClientWithHeartbeatingForRangeKeyTable.tryAcquireLock(ACQUIRE_LOCK_OPTIONS_WITH_NO_WAIT_SORT_1));
    }

    @Test
    public void testAcquireLockLeaveData() throws IOException, LockNotGrantedException, InterruptedException {
        final String data = "testAcquireLockLeaveData";
        LockItem item = this.lockClient.acquireLock(AcquireLockOptions.builder("testKey1").withData(ByteBuffer.wrap(data.getBytes())).build());
        assertEquals(item.getPartitionKey(), "testKey1");

        this.lockClient.releaseLock(ReleaseLockOptions.builder(item).withDeleteLock(false).build());
        assertEquals(Optional.empty(), this.lockClient.getLock("testKey1", Optional.empty()));
        assertTrue(this.lockClient.getLockFromDynamoDB(new GetLockOptions.GetLockOptionsBuilder("testKey1").build()).get().isReleased());

        item = this.lockClient.acquireLock(ACQUIRE_LOCK_OPTIONS_REPLACE_DATA_FALSE);

        assertEquals(data, new String(item.getData().get().array()));
        item.close();
    }

    @Test
    public void testAcquireLockLeaveData_rvnChanged() throws IOException, LockNotGrantedException, InterruptedException {
        final String data = "testAcquireLockLeaveData";
        LockItem item = this.lockClient.acquireLock(AcquireLockOptions.builder("testKey1").withData(ByteBuffer.wrap(data.getBytes())).build());
        assertEquals(item.getPartitionKey(), "testKey1");

        this.lockClient.releaseLock(ReleaseLockOptions.builder(item).withDeleteLock(false).build());
        assertEquals(Optional.empty(), this.lockClient.getLock("testKey1", Optional.empty()));
        item = this.lockClient.getLockFromDynamoDB(new GetLockOptions.GetLockOptionsBuilder("testKey1").build()).get();
        assertTrue(item.isReleased());

        // report a different RVN from whats in ddb
        item.updateRecordVersionNumber(UUID.randomUUID().toString(), 0 ,0);
        doReturn(Optional.of(item)).when(lockClient).getLockFromDynamoDB(Mockito.any(GetLockOptions.class));

        try {
            item = this.lockClient.acquireLock(ACQUIRE_LOCK_OPTIONS_REPLACE_DATA_FALSE);
            assertEquals(data, new String(item.getData().get().array()));
            item.close();
        } catch (LockNotGrantedException e) {
            fail("Lock should not fail to acquire due to incorrect RVN - consistent data not enabled");
        }
    }

    @Test
    public void testAcquireLockLeaveDataWithSortKey() throws IOException, LockNotGrantedException, InterruptedException {

        final String data = "testAcquireLockLeaveData";
        LockItem item = this.lockClientForRangeKeyTable.acquireLock(AcquireLockOptions.builder("testKey1").withSortKey("1").withData
                (ByteBuffer.wrap(data.getBytes())).build());
        assertEquals(item.getPartitionKey(), "testKey1");
        assertEquals(item.getSortKey(), Optional.of("1"));

        this.lockClientForRangeKeyTable.releaseLock(ReleaseLockOptions.builder(item).withDeleteLock(false).build());
        assertEquals(Optional.empty(), this.lockClientForRangeKeyTable.getLock("testKey1", Optional.of("1")));
        assertTrue(this.lockClientForRangeKeyTable.getLockFromDynamoDB(new GetLockOptions.GetLockOptionsBuilder("testKey1").withSortKey("1").build()).get().isReleased());

        item = this.lockClientForRangeKeyTable.acquireLock(ACQUIRE_LOCK_OPTIONS_REPLACE_DATA_FALSE_SORT_1);

        assertEquals(data, new String(item.getData().get().array()));
        item.close();
    }

    @Test
    public void testAcquireLockLeaveDataWithSortKey_rvnChanged() throws IOException, LockNotGrantedException, InterruptedException {

        final String data = "testAcquireLockLeaveData";
        LockItem item = this.lockClientForRangeKeyTable.acquireLock(AcquireLockOptions.builder("testKey1").withSortKey("1").withData
                (ByteBuffer.wrap(data.getBytes())).build());
        assertEquals(item.getPartitionKey(), "testKey1");
        assertEquals(item.getSortKey(), Optional.of("1"));

        this.lockClientForRangeKeyTable.releaseLock(ReleaseLockOptions.builder(item).withDeleteLock(false).build());
        assertEquals(Optional.empty(), this.lockClientForRangeKeyTable.getLock("testKey1", Optional.of("1")));
        item = this.lockClientForRangeKeyTable.getLockFromDynamoDB(new GetLockOptions.GetLockOptionsBuilder("testKey1").withSortKey("1").build()).get();
        assertTrue(item.isReleased());

        // report a different RVN from whats in ddb
        item.updateRecordVersionNumber(UUID.randomUUID().toString(), 0 ,0);
        doReturn(Optional.of(item)).when(lockClientForRangeKeyTable).getLockFromDynamoDB(Mockito.any(GetLockOptions.class));

        try {
            item = this.lockClientForRangeKeyTable.acquireLock(ACQUIRE_LOCK_OPTIONS_REPLACE_DATA_FALSE_SORT_1);
            assertEquals(data, new String(item.getData().get().array()));
            item.close();
        } catch (LockNotGrantedException e) {
            fail("Lock should not fail to acquire due to incorrect RVN - consistent data not enabled");
        }
    }

    @Test
    public void testAcquireLockLeaveDataConsistentData() throws IOException, LockNotGrantedException, InterruptedException {

        final String data = "testAcquireLockLeaveData";
        LockItem item = this.lockClient.acquireLock(AcquireLockOptions.builder("testKey1").withData(ByteBuffer.wrap(data.getBytes())).build());
        assertEquals(item.getPartitionKey(), "testKey1");

        this.lockClient.releaseLock(ReleaseLockOptions.builder(item).withDeleteLock(false).build());
        assertEquals(Optional.empty(), this.lockClient.getLock("testKey1", Optional.empty()));
        assertTrue(this.lockClient.getLockFromDynamoDB(new GetLockOptions.GetLockOptionsBuilder("testKey1").build()).get().isReleased());

        item = this.lockClient.acquireLock(ACQUIRE_LOCK_OPTIONS_REPLACE_DATA_FALSE_CONSISTENT_DATA_TRUE);

        assertEquals(data, new String(item.getData().get().array()));
        item.close();
    }


    @Test
    public void testAcquireLockLeaveDataConsistentData_rvnChanged() throws IOException, LockNotGrantedException, InterruptedException {

        final String data = "testAcquireLockLeaveData";
        LockItem item = this.lockClient.acquireLock(AcquireLockOptions.builder("testKey1").withData(ByteBuffer.wrap(data.getBytes())).build());
        assertEquals(item.getPartitionKey(), "testKey1");

        this.lockClient.releaseLock(ReleaseLockOptions.builder(item).withDeleteLock(false).build());
        assertEquals(Optional.empty(), this.lockClient.getLock("testKey1", Optional.empty()));
        item = this.lockClient.getLockFromDynamoDB(new GetLockOptions.GetLockOptionsBuilder("testKey1").build()).get();
        assertTrue(item.isReleased());

        // report a different RVN from whats in ddb
        item.updateRecordVersionNumber(UUID.randomUUID().toString(), 0 ,0);
        doReturn(Optional.of(item)).when(lockClient).getLockFromDynamoDB(Mockito.any(GetLockOptions.class));

        try {
            item = this.lockClient.acquireLock(ACQUIRE_LOCK_OPTIONS_REPLACE_DATA_FALSE_CONSISTENT_DATA_TRUE);
            fail("Lock should fail to acquire due to incorrect RVN with consistent data enabled");
        } catch (LockNotGrantedException e) {
        }
    }

    @Test
    public void testAcquireLockLeaveDataConsistentDataWithSortKey() throws IOException, LockNotGrantedException, InterruptedException {

        final String data = "testAcquireLockLeaveData";
        LockItem item = this.lockClientForRangeKeyTable.acquireLock(AcquireLockOptions.builder("testKey1").withSortKey("1")
                .withData(ByteBuffer.wrap(data.getBytes())).build());
        assertEquals(item.getPartitionKey(), "testKey1");
        assertEquals(item.getSortKey(), Optional.of("1"));

        this.lockClientForRangeKeyTable.releaseLock(ReleaseLockOptions.builder(item).withDeleteLock(false).build());
        assertEquals(Optional.empty(), this.lockClientForRangeKeyTable.getLock("testKey1", Optional.of("1")));
        assertTrue(this.lockClientForRangeKeyTable.getLockFromDynamoDB(new GetLockOptions.GetLockOptionsBuilder("testKey1")
                .withSortKey("1").build()).get().isReleased());

        item = this.lockClientForRangeKeyTable.acquireLock(ACQUIRE_LOCK_OPTIONS_REPLACE_DATA_FALSE_CONSISTENT_DATA_TRUE_SORT_1);

        assertEquals(data, new String(item.getData().get().array()));
        item.close();
    }

    @Test
    public void testAcquireLockLeaveDataConsistentDataWithSortKey_rvnChanged() throws IOException, LockNotGrantedException,
            InterruptedException {

        final String data = "testAcquireLockLeaveData";
        LockItem item = this.lockClientForRangeKeyTable.acquireLock(AcquireLockOptions.builder("testKey1").withSortKey("1")
                .withData(ByteBuffer.wrap(data.getBytes())).build());
        assertEquals(item.getPartitionKey(), "testKey1");
        assertEquals(item.getSortKey(), Optional.of("1"));

        this.lockClientForRangeKeyTable.releaseLock(ReleaseLockOptions.builder(item).withDeleteLock(false).build());
        assertEquals(Optional.empty(), this.lockClientForRangeKeyTable.getLock("testKey1", Optional.of("1")));
        item = this.lockClientForRangeKeyTable.getLockFromDynamoDB(new GetLockOptions.GetLockOptionsBuilder("testKey1").withSortKey("1").build()).get();
        assertTrue(item.isReleased());

        // report a different RVN from whats in ddb
        item.updateRecordVersionNumber(UUID.randomUUID().toString(), 0 ,0);
        doReturn(Optional.of(item)).when(lockClientForRangeKeyTable).getLockFromDynamoDB(Mockito.any(GetLockOptions.class));

        try {
            item = this.lockClientForRangeKeyTable.acquireLock(ACQUIRE_LOCK_OPTIONS_REPLACE_DATA_FALSE_CONSISTENT_DATA_TRUE_SORT_1);
            fail("Lock should fail to acquire due to incorrect RVN with consistent data enabled");
        } catch (LockNotGrantedException e) {
        }
    }


    @Test
    public void testAcquireLockLeaveDataWhenUpdateExistingLockTrue() throws IOException, LockNotGrantedException, InterruptedException {

        final String data = "testAcquireLockLeaveData";
        final String lockPartitionKey = "testKey1";
        LockItem item = this.lockClient.acquireLock(AcquireLockOptions.builder(lockPartitionKey).withData(ByteBuffer.wrap(data.getBytes())).build());
        assertEquals(item.getPartitionKey(), lockPartitionKey);

        this.lockClient.releaseLock(ReleaseLockOptions.builder(item).withDeleteLock(false).build());
        assertEquals(Optional.empty(), this.lockClient.getLock(lockPartitionKey, Optional.empty()));
        assertTrue(this.lockClient.getLockFromDynamoDB(new GetLockOptions.GetLockOptionsBuilder(lockPartitionKey).build()).get().isReleased());

        item = this.lockClient.acquireLock(ACQUIRE_LOCK_OPTIONS_REPLACE_DATA_FALSE_UPDATE_EXISTING_TRUE);

        assertEquals(data, new String(item.getData().get().array()));
        item.close();
    }

    @Test
    public void testAcquireLockLeaveDataWhenUpdateExistingLockTrue_rvnChanged() throws IOException, LockNotGrantedException,
            InterruptedException {

        final String data = "testAcquireLockLeaveData";
        final String lockPartitionKey = "testKey1";
        LockItem item = this.lockClient.acquireLock(AcquireLockOptions.builder(lockPartitionKey).withData(ByteBuffer.wrap(data.getBytes())).build());
        assertEquals(item.getPartitionKey(), lockPartitionKey);

        this.lockClient.releaseLock(ReleaseLockOptions.builder(item).withDeleteLock(false).build());
        assertEquals(Optional.empty(), this.lockClient.getLock(lockPartitionKey, Optional.empty()));
        item = this.lockClient.getLockFromDynamoDB(new GetLockOptions.GetLockOptionsBuilder(lockPartitionKey).build()).get();
        assertTrue(item.isReleased());

        // report a different RVN from whats in ddb
        item.updateRecordVersionNumber(UUID.randomUUID().toString(), 0 ,0);
        doReturn(Optional.of(item)).when(lockClient).getLockFromDynamoDB(Mockito.any(GetLockOptions.class));

        try {
            item = this.lockClient.acquireLock(ACQUIRE_LOCK_OPTIONS_REPLACE_DATA_FALSE_UPDATE_EXISTING_TRUE);
            assertEquals(data, new String(item.getData().get().array()));
            item.close();
        } catch (LockNotGrantedException e) {
            fail("Lock should not fail to acquire due to incorrect RVN - consistent data not enabled");
        }
    }

    @Test
    public void testAcquireLockLeaveDataWhenUpdateExistingLockTrueWithSortKey() throws IOException, LockNotGrantedException,
            InterruptedException {

        final String data = "testAcquireLockLeaveData";
        final String lockPartitionKey = "testKey1";
        LockItem item = this.lockClientForRangeKeyTable.acquireLock(AcquireLockOptions.builder(lockPartitionKey).withSortKey("1")
                .withData(ByteBuffer.wrap(data.getBytes())).build());

        assertEquals(item.getPartitionKey(), lockPartitionKey);
        assertEquals(item.getSortKey(), Optional.of("1"));

        this.lockClientForRangeKeyTable.releaseLock(ReleaseLockOptions.builder(item).withDeleteLock(false).build());
        assertEquals(Optional.empty(), this.lockClientForRangeKeyTable.getLock(lockPartitionKey, Optional.of("1")));
        assertTrue(this.lockClientForRangeKeyTable.getLockFromDynamoDB(new GetLockOptions.GetLockOptionsBuilder(lockPartitionKey)
                .withSortKey("1").build()).get().isReleased());

        item = this.lockClientForRangeKeyTable.acquireLock(ACQUIRE_LOCK_OPTIONS_REPLACE_DATA_FALSE_UPDATE_EXISTING_TRUE_SORT_1);

        assertEquals(data, new String(item.getData().get().array()));
        item.close();
    }

    @Test
    public void testAcquireLockLeaveDataWhenUpdateExistingLockTrueWithSortKey_rvnChanged() throws IOException, LockNotGrantedException,
            InterruptedException {

        final String data = "testAcquireLockLeaveData";
        final String lockPartitionKey = "testKey1";
        LockItem item = this.lockClientForRangeKeyTable.acquireLock(AcquireLockOptions.builder(lockPartitionKey).withSortKey("1")
                .withData(ByteBuffer.wrap(data.getBytes())).build());

        assertEquals(item.getPartitionKey(), lockPartitionKey);
        assertEquals(item.getSortKey(), Optional.of("1"));

        this.lockClientForRangeKeyTable.releaseLock(ReleaseLockOptions.builder(item).withDeleteLock(false).build());
        assertEquals(Optional.empty(), this.lockClientForRangeKeyTable.getLock(lockPartitionKey, Optional.of("1")));
        item = this.lockClientForRangeKeyTable.getLockFromDynamoDB(new GetLockOptions.GetLockOptionsBuilder(lockPartitionKey).withSortKey("1").build()).get();
        assertTrue(item.isReleased());

        // report a different RVN from whats in ddb
        item.updateRecordVersionNumber(UUID.randomUUID().toString(), 0 ,0);
        doReturn(Optional.of(item)).when(lockClient).getLockFromDynamoDB(Mockito.any(GetLockOptions.class));

        try {
            item = this.lockClientForRangeKeyTable.acquireLock(ACQUIRE_LOCK_OPTIONS_REPLACE_DATA_FALSE_UPDATE_EXISTING_TRUE_SORT_1);
            assertEquals(data, new String(item.getData().get().array()));
            item.close();
        } catch (LockNotGrantedException e) {
            fail("Lock should not fail to acquire due to incorrect RVN - consistent data not enabled");
        }
    }


    @Test
    public void testAcquireLockLeaveDataConsistentDataWhenUpdateExistingLockTrue() throws IOException, LockNotGrantedException,
            InterruptedException {

        final String data = "testAcquireLockLeaveData";
        final String lockPartitionKey = "testKey1";
        LockItem item = this.lockClient.acquireLock(AcquireLockOptions.builder(lockPartitionKey).withData(ByteBuffer.wrap(data.getBytes())).build());
        assertEquals(item.getPartitionKey(), lockPartitionKey);

        this.lockClient.releaseLock(ReleaseLockOptions.builder(item).withDeleteLock(false).build());
        assertEquals(Optional.empty(), this.lockClient.getLock(lockPartitionKey, Optional.empty()));
        assertTrue(this.lockClient.getLockFromDynamoDB(new GetLockOptions.GetLockOptionsBuilder(lockPartitionKey).build()).get().isReleased());

        item = this.lockClient.acquireLock(ACQUIRE_LOCK_OPTIONS_REPLACE_DATA_FALSE_UPDATE_EXISTING_TRUE_CONSISTENT_DATA_TRUE);

        assertEquals(data, new String(item.getData().get().array()));
        item.close();
    }

    @Test
    public void testAcquireLockLeaveDataConsistentDataWhenUpdateExistingLockTrue_rvnChanged() throws IOException, LockNotGrantedException,
            InterruptedException {

        final String data = "testAcquireLockLeaveData";
        final String lockPartitionKey = "testKey1";
        LockItem item = this.lockClient.acquireLock(AcquireLockOptions.builder(lockPartitionKey).withData(ByteBuffer.wrap(data.getBytes())).build());
        assertEquals(item.getPartitionKey(), lockPartitionKey);

        this.lockClient.releaseLock(ReleaseLockOptions.builder(item).withDeleteLock(false).build());
        assertEquals(Optional.empty(), this.lockClient.getLock(lockPartitionKey, Optional.empty()));
        item = this.lockClient.getLockFromDynamoDB(new GetLockOptions.GetLockOptionsBuilder(lockPartitionKey).build()).get();
        assertTrue(item.isReleased());

        // report a different RVN from whats in ddb
        item.updateRecordVersionNumber(UUID.randomUUID().toString(), 0 ,0);
        doReturn(Optional.of(item)).when(lockClient).getLockFromDynamoDB(Mockito.any(GetLockOptions.class));

        try {
            item = this.lockClient.acquireLock(ACQUIRE_LOCK_OPTIONS_REPLACE_DATA_FALSE_UPDATE_EXISTING_TRUE_CONSISTENT_DATA_TRUE);
            fail("Lock should fail to acquire due to incorrect RVN with consistent data enabled");
        } catch (LockNotGrantedException e) {
        }
    }

    @Test
    public void testAcquireLockLeaveDataConsistentDataWhenUpdateExistingLockTrueWithSortKey() throws IOException, LockNotGrantedException,
            InterruptedException {

        final String data = "testAcquireLockLeaveData";
        final String lockPartitionKey = "testKey1";
        LockItem item = this.lockClientForRangeKeyTable.acquireLock(AcquireLockOptions.builder(lockPartitionKey).withSortKey("1")
                .withData(ByteBuffer.wrap(data.getBytes())).build());
        assertEquals(item.getPartitionKey(), lockPartitionKey);
        assertEquals(item.getSortKey(), Optional.of("1"));

        this.lockClientForRangeKeyTable.releaseLock(ReleaseLockOptions.builder(item).withDeleteLock(false).build());
        assertEquals(Optional.empty(), this.lockClientForRangeKeyTable.getLock(lockPartitionKey, Optional.of("1")));
        assertTrue(this.lockClientForRangeKeyTable.getLockFromDynamoDB(new GetLockOptions.GetLockOptionsBuilder(lockPartitionKey).withSortKey("1").build()).get().isReleased());

        item = this.lockClientForRangeKeyTable.acquireLock(ACQUIRE_LOCK_OPTIONS_REPLACE_DATA_FALSE_UPDATE_EXISTING_TRUE_CONSISTENT_DATA_TRUE_SORT_1);

        assertEquals(data, new String(item.getData().get().array()));
        item.close();
    }

    @Test
    public void testAcquireLockLeaveDataConsistentDataWhenUpdateExistingLockTrueWithSortKey_rvnChanged() throws IOException,
            LockNotGrantedException, InterruptedException {

        final String data = "testAcquireLockLeaveData";
        final String lockPartitionKey = "testKey1";
        LockItem item = this.lockClientForRangeKeyTable.acquireLock(AcquireLockOptions.builder(lockPartitionKey).withSortKey("1")
                .withData(ByteBuffer.wrap(data.getBytes())).build());
        assertEquals(item.getPartitionKey(), lockPartitionKey);
        assertEquals(item.getSortKey(), Optional.of("1"));

        this.lockClientForRangeKeyTable.releaseLock(ReleaseLockOptions.builder(item).withDeleteLock(false).build());
        assertEquals(Optional.empty(), this.lockClientForRangeKeyTable.getLock(lockPartitionKey, Optional.of("1")));
        item = this.lockClientForRangeKeyTable.getLockFromDynamoDB(new GetLockOptions.GetLockOptionsBuilder(lockPartitionKey).withSortKey("1").build()).get();
        assertTrue(item.isReleased());

        // report a different RVN from whats in ddb
        item.updateRecordVersionNumber(UUID.randomUUID().toString(), 0 ,0);
        doReturn(Optional.of(item)).when(lockClientForRangeKeyTable).getLockFromDynamoDB(Mockito.any(GetLockOptions.class));

        try {
            item = this.lockClientForRangeKeyTable.acquireLock(ACQUIRE_LOCK_OPTIONS_REPLACE_DATA_FALSE_UPDATE_EXISTING_TRUE_CONSISTENT_DATA_TRUE_SORT_1);
            fail("Lock should fail to acquire due to incorrect RVN with consistent data enabled");
        } catch (LockNotGrantedException e) {
        }
    }

    @Test
    public void testAcquireLockMustExistWhenNotUpdateExistingLock_LockDoesNotExist() throws IOException, LockNotGrantedException,
            InterruptedException {

        try {
            this.lockClient.acquireLock(ACQUIRE_LOCK_OPTIONS_ONLY_IF_LOCK_EXIST_UPDATE_FALSE);
            fail("Acquire lock should fail with LockNotGrantedException");
        } catch (LockNotGrantedException e) {
            //expected exception
        }
    }

    @Test
    public void testAcquireLockMustExistWhenNotUpdateExistingLockConsistentData_LockDoesNotExist() throws IOException,
            LockNotGrantedException, InterruptedException {

        try {
            this.lockClient.acquireLock(ACQUIRE_LOCK_OPTIONS_ONLY_IF_LOCK_EXIST_UPDATE_FALSE_CONSISTENT_DATA_TRUE);
            fail("Acquire lock should fail with LockNotGrantedException");
        } catch (LockNotGrantedException e) {
            //expected exception
        }
    }

    @Test
    public void testAcquireLockMustExistWhenNotUpdateExistingLockWithSortKey_LockDoesNotExist() throws IOException, LockNotGrantedException,
            InterruptedException {

        try {
            this.lockClientForRangeKeyTable.acquireLock(ACQUIRE_LOCK_OPTIONS_ONLY_IF_LOCK_EXIST_UPDATE_FALSE_SORT_1);
            fail("Acquire lock should fail with LockNotGrantedException");
        } catch (LockNotGrantedException e) {
            //expected exception
        }
    }

    @Test
    public void testAcquireLockMustExistWhenNotUpdateExistingLockConsistentDataWithSortKey_LockDoesNotExist() throws IOException,
            LockNotGrantedException, InterruptedException {

        try {
            this.lockClientForRangeKeyTable.acquireLock(ACQUIRE_LOCK_OPTIONS_ONLY_IF_LOCK_EXIST_UPDATE_FALSE_CONSISTENT_DATA_TRUE_SORT_1);
            fail("Acquire lock should fail with LockNotGrantedException");
        } catch (LockNotGrantedException e) {
            //expected exception
        }
    }

    @Test
    public void testAcquireLockMustExistWhenUpdateExistingLock_LockDoesNotExist() throws IOException, LockNotGrantedException,
            InterruptedException {

        try {
            this.lockClient.acquireLock(ACQUIRE_LOCK_OPTIONS_ONLY_IF_LOCK_EXIST_UPDATE_TRUE);
            fail("Acquire lock should fail with LockNotGrantedException");
        } catch (LockNotGrantedException e) {
            //expected exception
        }
    }

    @Test
    public void testAcquireLockMustExistWhenUpdateExistingLockConsistentData_LockDoesNotExist() throws IOException, LockNotGrantedException,
            InterruptedException {

        try {
            this.lockClient.acquireLock(ACQUIRE_LOCK_OPTIONS_ONLY_IF_LOCK_EXIST_UPDATE_TRUE_CONSISTENT_DATA_TRUE);
            fail("Acquire lock should fail with LockNotGrantedException");
        } catch (LockNotGrantedException e) {
            //expected exception
        }
    }

    @Test
    public void testAcquireLockMustExistWhenUpdateExistingLockWithSortKey_LockDoesNotExist() throws IOException, LockNotGrantedException,
            InterruptedException {

        try {
            this.lockClientForRangeKeyTable.acquireLock(ACQUIRE_LOCK_OPTIONS_ONLY_IF_LOCK_EXIST_UPDATE_TRUE_SORT_1);
            fail("Acquire lock should fail with LockNotGrantedException");
        } catch (LockNotGrantedException e) {
            //expected exception
        }
    }

    @Test
    public void testAcquireLockMustExistWhenUpdateExistingLockConsistentDataWithSortKey_LockDoesNotExist() throws IOException,
            LockNotGrantedException, InterruptedException {

        try {
            this.lockClientForRangeKeyTable.acquireLock(ACQUIRE_LOCK_OPTIONS_ONLY_IF_LOCK_EXIST_UPDATE_TRUE_CONSISTENT_DATA_TRUE_SORT_1);
            fail("Acquire lock should fail with LockNotGrantedException");
        } catch (LockNotGrantedException e) {
            //expected exception
        }
    }

    @Test
    public void testAcquireLockMustExistWhenNotUpdateExistingLock_LockExists() throws IOException, LockNotGrantedException,
            InterruptedException {

        final String data = "testAcquireLockMustExist";
        LockItem item = this.lockClient.acquireLock(AcquireLockOptions.builder("testKey1").withData(ByteBuffer.wrap(data.getBytes())).build());
        assertEquals(item.getPartitionKey(), "testKey1");

        this.lockClient.releaseLock(ReleaseLockOptions.builder(item).withDeleteLock(false).build());
        assertEquals(Optional.empty(), this.lockClient.getLock("testKey1", Optional.empty()));
        assertTrue(this.lockClient.getLockFromDynamoDB(new GetLockOptions.GetLockOptionsBuilder("testKey1").build()).get().isReleased());

        item = this.lockClient.acquireLock(ACQUIRE_LOCK_OPTIONS_ONLY_IF_LOCK_EXIST_UPDATE_FALSE);
        assertEquals(data, new String(item.getData().get().array()));
        item.close();
    }

    @Test
    public void testAcquireLockMustExistWhenNotUpdateExistingLockConsistentData_LockExists() throws IOException, LockNotGrantedException,
            InterruptedException {

        final String data = "testAcquireLockMustExist";
        LockItem item = this.lockClient.acquireLock(AcquireLockOptions.builder("testKey1").withData(ByteBuffer.wrap(data.getBytes())).build());
        assertEquals(item.getPartitionKey(), "testKey1");

        this.lockClient.releaseLock(ReleaseLockOptions.builder(item).withDeleteLock(false).build());
        assertEquals(Optional.empty(), this.lockClient.getLock("testKey1", Optional.empty()));
        assertTrue(this.lockClient.getLockFromDynamoDB(new GetLockOptions.GetLockOptionsBuilder("testKey1").build()).get().isReleased());

        item = this.lockClient.acquireLock(ACQUIRE_LOCK_OPTIONS_ONLY_IF_LOCK_EXIST_UPDATE_FALSE_CONSISTENT_DATA_TRUE);
        assertEquals(data, new String(item.getData().get().array()));
        item.close();
    }

    @Test
    public void testAcquireLockMustExistWhenNotUpdateExistingLockWithSortKey_LockExists() throws IOException, LockNotGrantedException,
            InterruptedException {

        final String data = "testAcquireLockMustExist";
        LockItem item = this.lockClientForRangeKeyTable.acquireLock(AcquireLockOptions.builder("testKey1").withSortKey("1").withData(ByteBuffer.wrap(data.getBytes())).build());
        assertEquals(item.getPartitionKey(), "testKey1");
        assertEquals(item.getSortKey(), Optional.of("1"));

        this.lockClientForRangeKeyTable.releaseLock(ReleaseLockOptions.builder(item).withDeleteLock(false).build());
        assertEquals(Optional.empty(), this.lockClientForRangeKeyTable.getLock("testKey1", Optional.of("1")));
        assertTrue(this.lockClientForRangeKeyTable.getLockFromDynamoDB(new GetLockOptions.GetLockOptionsBuilder("testKey1").withSortKey("1").build()).get().isReleased());

        item = this.lockClientForRangeKeyTable.acquireLock(ACQUIRE_LOCK_OPTIONS_ONLY_IF_LOCK_EXIST_UPDATE_FALSE_SORT_1);
        assertEquals(data, new String(item.getData().get().array()));
        item.close();
    }

    @Test
    public void testAcquireLockMustExistWhenNotUpdateExistingLockConsistentDataWithSortKey_LockExists() throws IOException,
            LockNotGrantedException, InterruptedException {

        final String data = "testAcquireLockMustExist";
        LockItem item = this.lockClientForRangeKeyTable.acquireLock(AcquireLockOptions.builder("testKey1").withSortKey("1").withData(ByteBuffer.wrap(data.getBytes())).build());
        assertEquals(item.getPartitionKey(), "testKey1");
        assertEquals(item.getSortKey(), Optional.of("1"));

        this.lockClientForRangeKeyTable.releaseLock(ReleaseLockOptions.builder(item).withDeleteLock(false).build());
        assertEquals(Optional.empty(), this.lockClientForRangeKeyTable.getLock("testKey1", Optional.of("1")));
        assertTrue(this.lockClientForRangeKeyTable.getLockFromDynamoDB(new GetLockOptions.GetLockOptionsBuilder("testKey1").withSortKey("1").build()).get().isReleased());

        item = this.lockClientForRangeKeyTable.acquireLock(ACQUIRE_LOCK_OPTIONS_ONLY_IF_LOCK_EXIST_UPDATE_FALSE_CONSISTENT_DATA_TRUE_SORT_1);
        assertEquals(data, new String(item.getData().get().array()));
        item.close();
    }

    @Test
    public void testAcquireLockMustExistWhenUpdateExistingLock_LockExist() throws IOException, LockNotGrantedException, InterruptedException {
        final String data = "testAcquireLockMustExist";
        LockItem item = this.lockClient.acquireLock(AcquireLockOptions.builder("testKey1").withData(ByteBuffer.wrap(data.getBytes())).build());
        assertEquals(item.getPartitionKey(), "testKey1");

        this.lockClient.releaseLock(ReleaseLockOptions.builder(item).withDeleteLock(false).build());
        assertEquals(Optional.empty(), this.lockClient.getLock("testKey1", Optional.empty()));
        assertTrue(this.lockClient.getLockFromDynamoDB(new GetLockOptions.GetLockOptionsBuilder("testKey1").build()).get().isReleased());

        item = this.lockClient.acquireLock(ACQUIRE_LOCK_OPTIONS_ONLY_IF_LOCK_EXIST_UPDATE_TRUE);
        assertEquals(data, new String(item.getData().get().array()));
        item.close();
    }

    @Test
    public void testAcquireLockMustExistWhenUpdateExistingLockConsistentData_LockExist() throws IOException, LockNotGrantedException, InterruptedException {
        final String data = "testAcquireLockMustExist";
        LockItem item = this.lockClient.acquireLock(AcquireLockOptions.builder("testKey1").withData(ByteBuffer.wrap(data.getBytes())).build());
        assertEquals(item.getPartitionKey(), "testKey1");

        this.lockClient.releaseLock(ReleaseLockOptions.builder(item).withDeleteLock(false).build());
        assertEquals(Optional.empty(), this.lockClient.getLock("testKey1", Optional.empty()));
        assertTrue(this.lockClient.getLockFromDynamoDB(new GetLockOptions.GetLockOptionsBuilder("testKey1").build()).get().isReleased());

        item = this.lockClient.acquireLock(ACQUIRE_LOCK_OPTIONS_ONLY_IF_LOCK_EXIST_UPDATE_TRUE_CONSISTENT_DATA_TRUE);
        assertEquals(data, new String(item.getData().get().array()));
        item.close();
    }

    @Test
    public void testAcquireLockMustExistWhenUpdateExistingLockWithSortKey_LockExist() throws IOException, LockNotGrantedException,
            InterruptedException {
        final String data = "testAcquireLockMustExist";
        LockItem item = this.lockClientForRangeKeyTable.acquireLock(AcquireLockOptions.builder("testKey1").withSortKey("1").withData(ByteBuffer.wrap(data.getBytes())).build());
        assertEquals(item.getPartitionKey(), "testKey1");
        assertEquals(item.getSortKey(), Optional.of("1"));

        this.lockClientForRangeKeyTable.releaseLock(ReleaseLockOptions.builder(item).withDeleteLock(false).build());
        assertEquals(Optional.empty(), this.lockClientForRangeKeyTable.getLock("testKey1", Optional.of("1")));
        assertTrue(this.lockClientForRangeKeyTable.getLockFromDynamoDB(new GetLockOptions.GetLockOptionsBuilder("testKey1").withSortKey("1").build()).get().isReleased());

        item = this.lockClientForRangeKeyTable.acquireLock(ACQUIRE_LOCK_OPTIONS_ONLY_IF_LOCK_EXIST_UPDATE_TRUE_SORT_1);
        assertEquals(data, new String(item.getData().get().array()));
        item.close();
    }

    @Test
    public void testAcquireLockMustExistWhenUpdateExistingLockConsistentDataWithSortKey_LockExist() throws IOException, LockNotGrantedException, InterruptedException {
        final String data = "testAcquireLockMustExist";
        LockItem item = this.lockClientForRangeKeyTable.acquireLock(AcquireLockOptions.builder("testKey1").withSortKey("1").withData(ByteBuffer.wrap(data.getBytes())).build());
        assertEquals(item.getPartitionKey(), "testKey1");
        assertEquals(item.getSortKey(), Optional.of("1"));

        this.lockClientForRangeKeyTable.releaseLock(ReleaseLockOptions.builder(item).withDeleteLock(false).build());
        assertEquals(Optional.empty(), this.lockClientForRangeKeyTable.getLock("testKey1", Optional.of("1")));
        assertTrue(this.lockClientForRangeKeyTable.getLockFromDynamoDB(new GetLockOptions.GetLockOptionsBuilder("testKey1").withSortKey("1").build()).get().isReleased());

        item = this.lockClientForRangeKeyTable.acquireLock(ACQUIRE_LOCK_OPTIONS_ONLY_IF_LOCK_EXIST_UPDATE_TRUE_CONSISTENT_DATA_TRUE_SORT_1);
        assertEquals(data, new String(item.getData().get().array()));
        item.close();
    }

    @Test
    public void testAcquireLockBasicWithUpdateExistingLockTrue() throws InterruptedException {
        LockItem item = this.lockClient.acquireLock(AcquireLockOptions.builder("testKey1").withUpdateExistingLockRecord(true).build());
        assertEquals(item.getPartitionKey(), "testKey1");
    }

    @Test
    public void testAcquireLockWhenLockIsReleasedAndUpdateExistingLockIsTruePreserveAttributesFromPreviousLock() throws InterruptedException, IOException {
        final AmazonDynamoDBLockClient lockClient1 = new AmazonDynamoDBLockClient(
                new AmazonDynamoDBLockClientOptions.AmazonDynamoDBLockClientOptionsBuilder(this.dynamoDBMock, RANGE_KEY_TABLE_NAME, INTEGRATION_TESTER_2).withLeaseDuration(3L).withHeartbeatPeriod(1L)
                        .withTimeUnit(TimeUnit.SECONDS).withSortKeyName("rangeKey").withCreateHeartbeatBackgroundThread(false).build());
        final AmazonDynamoDBLockClient lockClient2 = new AmazonDynamoDBLockClient(
                new AmazonDynamoDBLockClientOptions.AmazonDynamoDBLockClientOptionsBuilder(this.dynamoDBMock, RANGE_KEY_TABLE_NAME, INTEGRATION_TESTER_2).withLeaseDuration(3L).withHeartbeatPeriod(1L)
                        .withTimeUnit(TimeUnit.SECONDS).withSortKeyName("rangeKey").withCreateHeartbeatBackgroundThread(false).build());
        final String additionalValue = "doNotDelete";
        final String lockPartitionKey = "testKey1";
        final String lockSortKey = "1";
        final Map<String, AttributeValue> additional = new HashMap<>();
        additional.put(additionalValue, new AttributeValue().withS(additionalValue));
        //acquire first lock
        final RequestMetricCollector requestMetricCollector = Mockito.mock(RequestMetricCollector.class);
        final Optional<LockItem> lockItem1 = lockClient1.tryAcquireLock(AcquireLockOptions.builder(lockPartitionKey).withSortKey(lockSortKey).withDeleteLockOnRelease(false).withAdditionalAttributes(additional).build());

        assertNotEquals(Optional.empty(), lockItem1);
        lockClient1.releaseLock(lockItem1.get());
        //acquire the same lock released above
        final Optional<LockItem> lockItem2 = lockClient2.tryAcquireLock(AcquireLockOptions.builder(lockPartitionKey).withSortKey(lockSortKey).withRequestMetricCollector(requestMetricCollector).withUpdateExistingLockRecord(true).build());

        assertTrue(lockItem2.isPresent());
        assertEquals(INTEGRATION_TESTER_2, lockItem2.get().getOwnerName());

        /* Get the complete record for the lock to verify other fields are not replaced*/
        Map<String, AttributeValue> key = new HashMap<>();
        key.put(lockClient1Options.getPartitionKeyName(), new AttributeValue().withS(lockPartitionKey));
        key.put("rangeKey", new AttributeValue().withS(lockSortKey));
        GetItemResult result = lockClient1Options.getDynamoDBClient().getItem(new GetItemRequest().withTableName(RANGE_KEY_TABLE_NAME).withKey(key));
        Map<String, AttributeValue> currentLockRecord = result.getItem();

        //any values left from old locks should not be removed
        assertNotNull(currentLockRecord.get(additionalValue));
        String additionalValuesExpected = currentLockRecord.get(additionalValue).getS();
        assertEquals(additionalValue, additionalValuesExpected);

        lockClient1.close();
        lockClient2.close();
    }

    @Test
    public void testAcquireLockWithSortKeyWhenLockIsExpiredAndUpdateExistingLockIsTruePreserveAdditionalAttributesFromPreviousLock() throws InterruptedException, IOException {
        final AmazonDynamoDBLockClient lockClient1 = new AmazonDynamoDBLockClient(
                new AmazonDynamoDBLockClientOptions.AmazonDynamoDBLockClientOptionsBuilder(this.dynamoDBMock, RANGE_KEY_TABLE_NAME, INTEGRATION_TESTER_2).withLeaseDuration(3L).withHeartbeatPeriod(1L)
                .withTimeUnit(TimeUnit.SECONDS).withSortKeyName("rangeKey").withCreateHeartbeatBackgroundThread(false).build());
        final AmazonDynamoDBLockClient lockClient2 = new AmazonDynamoDBLockClient(
                new AmazonDynamoDBLockClientOptions.AmazonDynamoDBLockClientOptionsBuilder(this.dynamoDBMock, RANGE_KEY_TABLE_NAME, INTEGRATION_TESTER_2).withLeaseDuration(3L).withHeartbeatPeriod(1L)
                        .withTimeUnit(TimeUnit.SECONDS).withSortKeyName("rangeKey").withCreateHeartbeatBackgroundThread(false).build());
        //add additional values to verify they are preserved always
        final String additionalValue = "doNotDelete";
        final String lockPartitionKey = "testKey1";
        final String lockSortKey = "1";
        Map<String, AttributeValue> additional = new HashMap<>();
        additional.put(additionalValue, new AttributeValue().withS(additionalValue));

        final Optional<LockItem> lockItem1 = lockClient1.tryAcquireLock(AcquireLockOptions.builder(lockPartitionKey).withSortKey(lockSortKey).withDeleteLockOnRelease(false).withAdditionalAttributes(additional).build());
        assertNotEquals(Optional.empty(), lockItem1);

        final Optional<LockItem> lockItem2 = lockClient2.tryAcquireLock(AcquireLockOptions.builder(lockPartitionKey).withSortKey(lockSortKey).withUpdateExistingLockRecord(true).build());
        assertTrue(lockItem2.isPresent());
        assertEquals(INTEGRATION_TESTER_2, lockItem2.get().getOwnerName());

        /* Get the complete record for the lock to verify other fields are not replaced*/
        Map<String, AttributeValue> key = new HashMap<>();
        key.put(lockClient1Options.getPartitionKeyName(), new AttributeValue().withS(lockPartitionKey));
        key.put("rangeKey", new AttributeValue().withS(lockSortKey));
        GetItemResult result = this.dynamoDBMock.getItem(new GetItemRequest().withTableName(RANGE_KEY_TABLE_NAME).withKey(key));
        Map<String, AttributeValue> currentLockRecord = result.getItem();
        //any values left from old locks should not be removed
        assertNotNull(currentLockRecord.get(additionalValue));
        String additionalValuesExpected = currentLockRecord.get(additionalValue).getS();
        assertEquals(additionalValue, additionalValuesExpected);

        lockClient1.close();
        lockClient2.close();
    }

    @Test
    public void testAcquireLockWhenLockIsExpiredAndUpdateExistingLockIsTruePreserveAdditionalAttributesFromPreviousLock() throws InterruptedException, IOException {
        final AmazonDynamoDBLockClient lockClient1 = new AmazonDynamoDBLockClient(
                new AmazonDynamoDBLockClientOptions.AmazonDynamoDBLockClientOptionsBuilder(this.dynamoDBMock, TABLE_NAME, INTEGRATION_TESTER_2).withLeaseDuration(3L).withHeartbeatPeriod(1L)
                        .withTimeUnit(TimeUnit.SECONDS).withCreateHeartbeatBackgroundThread(false).build());
        final AmazonDynamoDBLockClient lockClient2 = new AmazonDynamoDBLockClient(
                new AmazonDynamoDBLockClientOptions.AmazonDynamoDBLockClientOptionsBuilder(this.dynamoDBMock, TABLE_NAME, INTEGRATION_TESTER_2).withLeaseDuration(3L).withHeartbeatPeriod(1L)
                        .withTimeUnit(TimeUnit.SECONDS).withCreateHeartbeatBackgroundThread(false).build());
        //add additional values to verify they are preserved always
        final String additionalValue = "doNotDelete";
        final String lockPartitionKey = "testKey1";
        Map<String, AttributeValue> additional = new HashMap<>();
        additional.put(additionalValue, new AttributeValue().withS(additionalValue));

        final Optional<LockItem> lockItem1 = lockClient1.tryAcquireLock(AcquireLockOptions.builder(lockPartitionKey).withDeleteLockOnRelease(false).withAdditionalAttributes(additional).build());
        assertNotEquals(Optional.empty(), lockItem1);

        /* try stealing the lock once it is expired */
        final Optional<LockItem> lockItem2 = lockClient2.tryAcquireLock(AcquireLockOptions.builder(lockPartitionKey).withUpdateExistingLockRecord(true).build());
        assertTrue(lockItem2.isPresent());
        assertEquals(INTEGRATION_TESTER_2, lockItem2.get().getOwnerName());

        /* Get the complete record for the lock to verify other fields are not replaced*/
        Map<String, AttributeValue> key = new HashMap<>();
        key.put(lockClient1Options.getPartitionKeyName(), new AttributeValue().withS(lockPartitionKey));

        GetItemResult result = this.dynamoDBMock.getItem(new GetItemRequest().withTableName(TABLE_NAME).withKey(key));
        Map<String, AttributeValue> currentLockRecord = result.getItem();
        //any values left from old locks should not be removed
        assertNotNull(currentLockRecord.get(additionalValue));
        String additionalValuesExpected = currentLockRecord.get(additionalValue).getS();
        assertEquals(additionalValue, additionalValuesExpected);

        lockClient1.close();
        lockClient2.close();
    }

    @Test
    public void testAcquireLockLeaveDataAfterClose() throws IOException, LockNotGrantedException, InterruptedException {

        final String data = "testAcquireLockLeaveData";

        final AcquireLockOptions options =
            AcquireLockOptions.builder("testKey1").withData(ByteBuffer.wrap(data.getBytes())).withRefreshPeriod(1L).withAdditionalTimeToWaitForLock(5L)
                .withTimeUnit(TimeUnit.SECONDS).withDeleteLockOnRelease(false).build();

        LockItem item = this.lockClient.acquireLock(options);
        assertEquals(item.getPartitionKey(), "testKey1");

        this.lockClient.close();
        this.lockClient = new AmazonDynamoDBLockClient(this.lockClient1Options);

        item = this.lockClient.acquireLock(ACQUIRE_LOCK_OPTIONS_REPLACE_DATA_FALSE);
        assertEquals(data, new String(item.getData().get().array()));

        item.close();
    }

    @Test
    public void testAcquireLockLeaveOrReplaceDataFromReleasedLock() throws IOException, LockNotGrantedException, InterruptedException {

        LockItem item = this.lockClient.acquireLock(AcquireLockOptions.builder("testKey1").build());

        this.lockClient.releaseLock(ReleaseLockOptions.builder(item).withDeleteLock(false).build());

        final String data = "testAcquireLockLeaveOrReplaceDataFromReleased";
        item = this.lockClient.acquireLock(AcquireLockOptions.builder("testKey1").withReplaceData(false).withData(ByteBuffer.wrap(data.getBytes())).build());

        assertEquals(data, new String(item.getData().get().array()));
        item.close();
    }

    @Test
    public void testAcquireLockLeaveOrReplaceDataFromAcquiredLock() throws IOException, LockNotGrantedException, InterruptedException {

        LockItem item = this.lockClient.acquireLock(AcquireLockOptions.builder("testKey1").build());

        final String data = "testAcquireLockLeaveOrReplaceData";
        item = this.lockClient.acquireLock(AcquireLockOptions.builder("testKey1").withReplaceData(false).withData(ByteBuffer.wrap(data.getBytes())).build());

        assertEquals(data, new String(item.getData().get().array()));
        item.close();
    }

    @Test
    public void testSendHeatbeatWithRangeKey() throws IOException, LockNotGrantedException, InterruptedException {

        final String data = new String("testSendHeartbeatLeaveData" + SECURE_RANDOM.nextDouble());

        final AmazonDynamoDBLockClient lockClient = new AmazonDynamoDBLockClient(
            AmazonDynamoDBLockClientOptions.builder(this.dynamoDBMock, RANGE_KEY_TABLE_NAME).withOwnerName(INTEGRATION_TESTER_2).withHeartbeatPeriod(2L).withLeaseDuration(30L)
                .withTimeUnit(TimeUnit.SECONDS).withCreateHeartbeatBackgroundThread(false).withSortKeyName("rangeKey").build());
        final LockItem item = lockClient.acquireLock(
            AcquireLockOptions.builder("testKey1").withData(ByteBuffer.wrap(data.getBytes())).withDeleteLockOnRelease(true).withReplaceData(true).withSortKey(TABLE_NAME).build());
        assertEquals(data, new String(item.getData().get().array()));

        assertEquals(Optional.empty(), lockClient.getLock("testKey1", Optional.of("nothing")));
        assertEquals(data, new String(lockClient.getLock("testKey1", Optional.of(TABLE_NAME)).get().getData().get().array()));
        lockClient.sendHeartbeat(item);
        assertEquals(data, new String(lockClient.getLock("testKey1", Optional.of(TABLE_NAME)).get().getData().get().array()));

        item.close();
        lockClient.close();
    }

    @Test
    public void testSendHeartbeatLeaveData() throws IOException, LockNotGrantedException, InterruptedException {

        final String data = new String("testSendHeartbeatLeaveData" + SECURE_RANDOM.nextDouble());

        final LockItem item = this.lockClient
            .acquireLock(AcquireLockOptions.builder("testKey1").withData(ByteBuffer.wrap(data.getBytes())).withDeleteLockOnRelease(true).withReplaceData(true).build());
        assertEquals(data, new String(item.getData().get().array()));

        assertEquals(data, new String(this.lockClient.getLock("testKey1", Optional.empty()).get().getData().get().array()));
        this.lockClient.sendHeartbeat(item);
        assertEquals(data, new String(this.lockClient.getLock("testKey1", Optional.empty()).get().getData().get().array()));

        item.close();
    }

    @Test
    public void testSendHeartbeatChangeData() throws IOException, LockNotGrantedException, InterruptedException {

        final String data1 = new String("testSendHeartbeatChangeData1" + SECURE_RANDOM.nextDouble());
        final String data2 = new String("testSendHeartbeatChangeData2" + SECURE_RANDOM.nextDouble());

        final LockItem item = this.lockClient
            .acquireLock(AcquireLockOptions.builder("testKey1").withData(ByteBuffer.wrap(data1.getBytes())).withDeleteLockOnRelease(true).withReplaceData(true).build());
        assertEquals(data1, new String(item.getData().get().array()));

        assertEquals(data1, new String(this.lockClient.getLockFromDynamoDB(GetLockOptions.builder("testKey1").build()).get().getData().get().array()));
        this.lockClient.sendHeartbeat(SendHeartbeatOptions.builder(item).withData(ByteBuffer.wrap(data2.getBytes())).build());
        assertEquals(data2, new String(this.lockClient.getLockFromDynamoDB(GetLockOptions.builder("testKey1").build()).get().getData().get().array()));

        item.close();
    }

    @Test
    public void testSendHeartbeatRemoveData() throws IOException, LockNotGrantedException, InterruptedException {

        final String data = new String("testSendHeartbeatLeaveData" + SECURE_RANDOM.nextDouble());
        final LockItem item = this.lockClient
            .acquireLock(AcquireLockOptions.builder("testKey1").withData(ByteBuffer.wrap(data.getBytes())).withDeleteLockOnRelease(true).withReplaceData(true).build());
        assertEquals(data, new String(item.getData().get().array()));

        assertEquals(data, new String(this.lockClient.getLockFromDynamoDB(GET_LOCK_OPTIONS_DELETE_ON_RELEASE).get().getData().get().array()));
        this.lockClient.sendHeartbeat(SendHeartbeatOptions.builder(item).withDeleteData(true).build());
        assertEquals(Optional.empty(), this.lockClient.getLockFromDynamoDB(GET_LOCK_OPTIONS_DELETE_ON_RELEASE).get().getData());

        item.close();
    }

    @Test
    public void testReleaseLockLeaveItem() throws IOException, LockNotGrantedException, InterruptedException {

        final String data = "testReleaseLockLeaveItem";
        LockItem item = this.lockClient
            .acquireLock(AcquireLockOptions.builder("testKey1").withData(ByteBuffer.wrap(data.getBytes())).withDeleteLockOnRelease(false).withReplaceData(true).build());
        assertEquals(data, new String(item.getData().get().array()));

        item.close();
        assertEquals(Optional.empty(), this.lockClient.getLock("testKey1", Optional.empty()));
        assertTrue(this.lockClient.getLockFromDynamoDB(GET_LOCK_OPTIONS_DO_NOT_DELETE_ON_RELEASE).get().isReleased());
        assertEquals(data, new String(this.lockClient.getLockFromDynamoDB(GET_LOCK_OPTIONS_DO_NOT_DELETE_ON_RELEASE).get().getData().get().array()));

        item = this.lockClient.acquireLock(AcquireLockOptions.builder("testKey1").withDeleteLockOnRelease(false).withReplaceData(false).build());
        assertEquals(data, new String(item.getData().get().array()));

        item.close();
    }

    @Test
    public void testReleaseLockLeaveItemAndChangeData() throws IOException, LockNotGrantedException, InterruptedException {

        final String data = "testReleaseLockLeaveItem";
        LockItem item = this.lockClient
            .acquireLock(AcquireLockOptions.builder("testKey1").withData(ByteBuffer.wrap(data.getBytes())).withDeleteLockOnRelease(false).withReplaceData(true).build());
        assertEquals(data, new String(item.getData().get().array()));

        this.lockClient.releaseLock(ReleaseLockOptions.builder(item).withDeleteLock(false).withData(ByteBuffer.wrap("newData".getBytes())).build());

        assertEquals(Optional.empty(), this.lockClient.getLock("testKey1", Optional.empty()));
        assertTrue(this.lockClient.getLockFromDynamoDB(GET_LOCK_OPTIONS_DO_NOT_DELETE_ON_RELEASE).get().isReleased());
        assertEquals("newData", new String(this.lockClient.getLockFromDynamoDB(GET_LOCK_OPTIONS_DO_NOT_DELETE_ON_RELEASE).get().getData().get().array()));

        item = this.lockClient.acquireLock(AcquireLockOptions.builder("testKey1").withDeleteLockOnRelease(false).withReplaceData(false).build());
        assertEquals("newData", new String(item.getData().get().array()));

        item.close();
    }

    @Test
    public void testReleaseLockRemoveItem() throws IOException, LockNotGrantedException, InterruptedException {
        final String data = "testReleaseLockRemoveItem";
        LockItem item = this.lockClient
            .acquireLock(AcquireLockOptions.builder("testKey1").withData(ByteBuffer.wrap(data.getBytes())).withDeleteLockOnRelease(true).withReplaceData(true).build());
        assertEquals(data, new String(item.getData().get().array()));

        item.close();
        item = this.lockClient.acquireLock(AcquireLockOptions.builder("testKey1").withDeleteLockOnRelease(true).withReplaceData(false).build());
        assertEquals(Optional.empty(), item.getData());

        item.close();
    }

    @Test
    public void testReleaseLockBestEffort() throws IOException, LockNotGrantedException, InterruptedException {
        final AmazonDynamoDBLockClient client = new AmazonDynamoDBLockClient(this.lockClient1Options);

        final LockItem item = client.acquireLock(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1);
        Mockito.doThrow(new AmazonClientException("Client exception releasing lock")).when(dynamoDBMock).deleteItem(Mockito.any(DeleteItemRequest.class));

        final ReleaseLockOptions options = ReleaseLockOptions.builder(item).withBestEffort(true).build();
        assertTrue(client.releaseLock(options));
        client.close();
    }

    @Test(expected = AmazonClientException.class)
    public void testReleaseLockNotBestEffort() throws IOException, LockNotGrantedException, InterruptedException {
        final AmazonDynamoDBLockClient client = new AmazonDynamoDBLockClient(AmazonDynamoDBLockClientOptions.builder(dynamoDBMock, TABLE_NAME).withOwnerName(LOCALHOST).build());

        final LockItem item = client.acquireLock(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1);
        Mockito.doThrow(new AmazonClientException("Client exception releasing lock")).when(dynamoDBMock).deleteItem(Mockito.any(DeleteItemRequest.class));

        client.releaseLock(item);
        client.close();
    }

    @Test
    public void testAcquireLockAfterTimeout() throws IOException, InterruptedException {
        final LockItem item = this.lockClient.acquireLock(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1);
        assertEquals(item.getPartitionKey(), "testKey1");

        assertNotEquals(Optional.empty(), this.lockClient.tryAcquireLock(ACQUIRE_LOCK_OPTIONS_5_SECONDS));
    }

    @Test
    public void testSucceedToAcquireLockAfterTimeout() throws IOException, LockNotGrantedException, InterruptedException {
        final LockItem item = this.lockClient.acquireLock(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1);
        assertEquals(item.getPartitionKey(), "testKey1");

        assertNotEquals(Optional.empty(), this.lockClient.tryAcquireLock(ACQUIRE_LOCK_OPTIONS_TEST_KEY_2));
        assertNotEquals(Optional.empty(), this.lockClient.acquireLock(ACQUIRE_LOCK_OPTIONS_5_SECONDS));
    }

    @Test
    public void testEmptyData() throws LockNotGrantedException, InterruptedException {
        final LockItem item = this.lockClient.acquireLock(AcquireLockOptions.builder("testKey1").build());
        assertEquals(item.getPartitionKey(), "testKey1");

        assertNotEquals(Optional.empty(), this.lockClient.tryAcquireLock(AcquireLockOptions.builder("testKey2").build()));

        assertEquals(Optional.empty(), this.lockClient.getLock("testKey1", Optional.empty()).get().getData());
        assertEquals(Optional.empty(), this.lockClient.getLock("testKey2", Optional.empty()).get().getData());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetupTooSmallHearbeatCheck() throws IOException {
        final AmazonDynamoDBLockClient client = new AmazonDynamoDBLockClient(
            AmazonDynamoDBLockClientOptions.builder(this.dynamoDBMock, TABLE_NAME).withOwnerName(INTEGRATION_TESTER).withLeaseDuration(5L).withHeartbeatPeriod(4L).withTimeUnit(TimeUnit.SECONDS)
                .withCreateHeartbeatBackgroundThread(true).build());
        client.close();
    }

    @Test
    public void testAcquiringSomeoneElsesLock() throws InterruptedException, IOException {
        final AmazonDynamoDBLockClient lockClient1 = new AmazonDynamoDBLockClient(this.lockClient1Options);
        final AmazonDynamoDBLockClient lockClient2 = new AmazonDynamoDBLockClient(
            new AmazonDynamoDBLockClientOptions.AmazonDynamoDBLockClientOptionsBuilder(this.dynamoDBMock, TABLE_NAME, INTEGRATION_TESTER_2).withLeaseDuration(3L).withHeartbeatPeriod(1L)
                .withTimeUnit(TimeUnit.SECONDS).withCreateHeartbeatBackgroundThread(false).build());
        final Optional<LockItem> lockItem1 = lockClient1.tryAcquireLock(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1);
        assertNotEquals(Optional.empty(), lockItem1);

        /* Steal lock */
        final Optional<LockItem> lockItem2 = lockClient2.tryAcquireLock(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1);
        assertTrue(lockItem2.isPresent());
        assertEquals(INTEGRATION_TESTER_2, lockItem2.get().getOwnerName());

        /* Release a lock that is now expired, should not actually succeed */
        assertFalse(lockClient1.releaseLock(lockItem1.get()));

        /* Make sure the lock is still there and owned by unitTester2 */
        final Optional<LockItem> lockItem3 = lockClient2.getLock("testKey1", Optional.empty());
        assertEquals(INTEGRATION_TESTER_2, lockItem3.get().getOwnerName());
        lockClient1.close();
        lockClient2.close();
    }

    @Test
    public void canReleaseExpiredLock() throws LockNotGrantedException, InterruptedException, IOException {
        final LockItem item = this.lockClient.acquireLock(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1);
        assertEquals(item.getPartitionKey(), "testKey1");

        Thread.sleep(4000);
        item.close();
        assertEquals(Optional.empty(), this.lockClient.getLock("testKey1", Optional.empty()));
    }

    @Test(expected = LockNotGrantedException.class)
    public void cannotHeartbeatExpiredLock() throws LockNotGrantedException, InterruptedException {
        final LockItem item = this.lockClient.acquireLock(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1);
        assertEquals(item.getPartitionKey(), "testKey1");

        Thread.sleep(4000);
        item.sendHeartBeat();
    }

    @Test
    public void cannotReleaseLockYouDontOwn() throws LockNotGrantedException, InterruptedException, IOException {
        final LockItem item = this.lockClient.acquireLock(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1);
        assertEquals(item.getPartitionKey(), "testKey1");

        final AmazonDynamoDBLockClient lockClient2 = new AmazonDynamoDBLockClient(
            AmazonDynamoDBLockClientOptions.builder(this.dynamoDBMock, TABLE_NAME).withOwnerName(INTEGRATION_TESTER_2).withLeaseDuration(30L).withHeartbeatPeriod(2L).withTimeUnit(TimeUnit.SECONDS)
                .withCreateHeartbeatBackgroundThread(false).build());
        final Optional<LockItem> item2 = lockClient2.getLock("testKey1", Optional.empty());
        assertFalse(lockClient2.releaseLock(item2.get()));
        lockClient2.close();
    }

    @Test(expected = LockNotGrantedException.class)
    public void cannotHeartbeatLockYouDontOwn() throws LockNotGrantedException, InterruptedException, IOException {
        final LockItem item = this.lockClient.acquireLock(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1);
        assertEquals(item.getPartitionKey(), "testKey1");

        final AmazonDynamoDBLockClient lockClient2 = new AmazonDynamoDBLockClient(
            AmazonDynamoDBLockClientOptions.builder(this.dynamoDBMock, TABLE_NAME).withOwnerName(INTEGRATION_TESTER_2).withLeaseDuration(30L).withHeartbeatPeriod(2L).withTimeUnit(TimeUnit.SECONDS)
                .withCreateHeartbeatBackgroundThread(false).build());
        final Optional<LockItem> item2 = lockClient2.getLock("testKey1", Optional.empty());
        lockClient2.sendHeartbeat(item2.get());
        lockClient2.close();
    }

    @Test
    public void testNullLock() {
        assertEquals(Optional.empty(), this.lockClient.getLock("lock1", Optional.empty()));
    }

    @Test
    public void testReleaseAllLocks() throws IOException, LockNotGrantedException, InterruptedException {
        this.lockClient.acquireLock(AcquireLockOptions.builder("Test 1").withData(ByteBuffer.wrap(TEST_DATA.getBytes())).build());
        this.lockClient.acquireLock(AcquireLockOptions.builder("Test 2").withData(ByteBuffer.wrap(TEST_DATA.getBytes())).build());
        this.lockClient.acquireLock(AcquireLockOptions.builder("Test 3").withData(ByteBuffer.wrap(TEST_DATA.getBytes())).build());
        assertNotEquals(Optional.empty(), this.lockClient.getLock("Test 1", Optional.empty()));
        assertNotEquals(Optional.empty(), this.lockClient.getLock("Test 2", Optional.empty()));
        assertNotEquals(Optional.empty(), this.lockClient.getLock("Test 3", Optional.empty()));
        this.lockClient.close();
        assertEquals(Optional.empty(), this.lockClient.getLock("Test 1", Optional.empty()));
        assertEquals(Optional.empty(), this.lockClient.getLock("Test 2", Optional.empty()));
        assertEquals(Optional.empty(), this.lockClient.getLock("Test 3", Optional.empty()));
    }

    @Test
    public void testEnsureLock() throws LockNotGrantedException, InterruptedException {
        final LockItem item = this.lockClient.acquireLock(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1);

        assertEquals(3000, this.lockClient.getLock("testKey1", Optional.empty()).get().getLeaseDuration());
        final String versionNumber = this.lockClient.getLock("testKey1", Optional.empty()).get().getRecordVersionNumber();

        /* We have the lock for 3 seconds, so this should do nothing */
        item.ensure(1, TimeUnit.SECONDS);
        assertEquals(3000, this.lockClient.getLock("testKey1", Optional.empty()).get().getLeaseDuration());
        assertEquals(versionNumber, this.lockClient.getLock("testKey1", Optional.empty()).get().getRecordVersionNumber());

        /* Now let's extend it so we have the lock for 4 seconds */
        item.ensure(4, TimeUnit.SECONDS);
        assertEquals(4000, this.lockClient.getLock("testKey1", Optional.empty()).get().getLeaseDuration());
        assertNotSame(versionNumber, this.lockClient.getLock("testKey1", Optional.empty()).get().getRecordVersionNumber());

        Thread.sleep(2400);

        /* Now the lock is about to expire, so we can still extend it for 2 seconds */
        item.ensure(2, TimeUnit.SECONDS);
        assertEquals(2000, this.lockClient.getLock("testKey1", Optional.empty()).get().getLeaseDuration());
        assertNotSame(versionNumber, this.lockClient.getLock("testKey1", Optional.empty()).get().getRecordVersionNumber());
    }

    @Test
    public void testGetLock() throws LockNotGrantedException, InterruptedException, IOException {
        this.lockClient.acquireLock(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1);
        assertNotEquals(Optional.empty(), this.lockClient.getLock("testKey1", Optional.empty()));
        this.lockClient.getLock("testKey1", Optional.empty()).get().close();
        assertEquals(Optional.empty(), this.lockClient.getLock("testKey1", Optional.empty()));
    }

    @Test
    public void testRangeKey() throws LockNotGrantedException, InterruptedException, IOException {
        final AmazonDynamoDBLockClient client1 = new AmazonDynamoDBLockClient(
            AmazonDynamoDBLockClientOptions.builder(this.dynamoDBMock, RANGE_KEY_TABLE_NAME).withOwnerName(INTEGRATION_TESTER_2).withLeaseDuration(5L).withHeartbeatPeriod(1L)
                .withTimeUnit(TimeUnit.SECONDS).withCreateHeartbeatBackgroundThread(true).withSortKeyName("rangeKey").build());
        final AmazonDynamoDBLockClient client2 = new AmazonDynamoDBLockClient(
            AmazonDynamoDBLockClientOptions.builder(this.dynamoDBMock, RANGE_KEY_TABLE_NAME).withOwnerName(INTEGRATION_TESTER_3).withLeaseDuration(5L).withHeartbeatPeriod(1L)
                .withTimeUnit(TimeUnit.SECONDS).withCreateHeartbeatBackgroundThread(true).withSortKeyName("rangeKey").build());

        final LockItem item = client1.acquireLock(AcquireLockOptions.builder("testKey1").withData(ByteBuffer.wrap(TEST_DATA.getBytes())).withSortKey("1").build());
        assertEquals(item.getPartitionKey(), "testKey1");

        assertEquals(client1.getLock("testKey1", Optional.of("1")).get().getSortKey().get(), "1");

        final Optional<LockItem> lock = client2.tryAcquireLock(
            AcquireLockOptions.builder("testKey1").withData(ByteBuffer.wrap(TEST_DATA.getBytes())).withReplaceData(false).withDeleteLockOnRelease(false).withRefreshPeriod(0L)
                .withAdditionalTimeToWaitForLock(0L).withTimeUnit(TimeUnit.MILLISECONDS).withSortKey("1").build());
        assertEquals(Optional.empty(), lock);
        item.close();
        assertNotEquals(Optional.empty(), client2.tryAcquireLock(
            AcquireLockOptions.builder("testKey1").withData(ByteBuffer.wrap(TEST_DATA.getBytes())).withReplaceData(false).withDeleteLockOnRelease(false).withRefreshPeriod(0L)
                .withAdditionalTimeToWaitForLock(0L).withTimeUnit(TimeUnit.MILLISECONDS).withSortKey("1").build()));
        client1.close();
        client2.close();
    }

    @Test
    public void testRangeKeyAcquiredAfterTimeout() throws LockNotGrantedException, InterruptedException, IOException {
        final AmazonDynamoDBLockClient client1 = new AmazonDynamoDBLockClient(
            AmazonDynamoDBLockClientOptions.builder(this.dynamoDBMock, RANGE_KEY_TABLE_NAME).withOwnerName(INTEGRATION_TESTER_2).withLeaseDuration(5L).withHeartbeatPeriod(1L)
                .withTimeUnit(TimeUnit.SECONDS).withCreateHeartbeatBackgroundThread(false).withSortKeyName("rangeKey").build());
        final AmazonDynamoDBLockClient client2 = new AmazonDynamoDBLockClient(
            AmazonDynamoDBLockClientOptions.builder(this.dynamoDBMock, RANGE_KEY_TABLE_NAME).withOwnerName(INTEGRATION_TESTER_3).withLeaseDuration(5L).withHeartbeatPeriod(1L)
                .withTimeUnit(TimeUnit.SECONDS).withCreateHeartbeatBackgroundThread(false).withSortKeyName("rangeKey").build());

        final LockItem item = client1.acquireLock(AcquireLockOptions.builder("testKey1").withData(ByteBuffer.wrap(TEST_DATA.getBytes())).withSortKey("1").build());
        assertEquals(item.getPartitionKey(), "testKey1");

        assertEquals(client1.getLock("testKey1", Optional.of("1")).get().getSortKey().get(), "1");
        assertNotEquals(Optional.empty(), client2.tryAcquireLock(
            AcquireLockOptions.builder("testKey1").withData(ByteBuffer.wrap(TEST_DATA.getBytes())).withReplaceData(false).withDeleteLockOnRelease(false).withRefreshPeriod(3L)
                .withAdditionalTimeToWaitForLock(1L).withTimeUnit(TimeUnit.SECONDS).withSortKey("1").build()));

        final LockItem item2 = client2.getLock("testKey1", Optional.of("1")).get();
        assertFalse("Expected lock to be valid", item2.isExpired());
        client1.close();
        client2.close();
    }

    @Test
    public void testAdditionalAttributes() throws LockNotGrantedException, InterruptedException, IOException {
        final Map<String, AttributeValue> additionalAttributes = new HashMap<String, AttributeValue>();
        additionalAttributes.put(TABLE_NAME, new AttributeValue().withS("ok"));
        LockItem item = this.lockClient
            .acquireLock(AcquireLockOptions.builder("testKey1").withData(ByteBuffer.wrap(TEST_DATA.getBytes())).withAdditionalAttributes(additionalAttributes).build());
        assertTrue(item.getAdditionalAttributes().get(TABLE_NAME).getS().equals("ok"));
        item = this.lockClient.getLock("testKey1", Optional.empty()).get();
        assertTrue(item.getAdditionalAttributes().get(TABLE_NAME).getS().equals("ok"));
        final AmazonDynamoDBLockClient client = new AmazonDynamoDBLockClient(this.lockClient1Options);
        item = client.getLock("testKey1", Optional.empty()).get();
        assertTrue(item.getAdditionalAttributes().get(TABLE_NAME).getS().equals("ok"));
        assertTrue(item.getAdditionalAttributes().equals(additionalAttributes));
        client.close();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidAttributesData() throws LockNotGrantedException, InterruptedException {
        this.testInvalidAttribute("data");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidAttributesKey() throws LockNotGrantedException, InterruptedException {
        this.testInvalidAttribute("key");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidAttributesLeaseDuration() throws LockNotGrantedException, InterruptedException {
        this.testInvalidAttribute("leaseDuration");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidAttributesRecordVersionNumber() throws LockNotGrantedException, InterruptedException {
        this.testInvalidAttribute("recordVersionNumber");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidAttributesOwnerName() throws LockNotGrantedException, InterruptedException {
        this.testInvalidAttribute("ownerName");
    }

    private void testInvalidAttribute(final String invalidAttribute) throws LockNotGrantedException, InterruptedException {
        final Map<String, AttributeValue> additionalAttributes = new HashMap<>();
        additionalAttributes.put(invalidAttribute, new AttributeValue().withS("ok"));
        this.lockClient.acquireLock(AcquireLockOptions.builder("testKey1").withData(ByteBuffer.wrap(TEST_DATA.getBytes())).withAdditionalAttributes(additionalAttributes).build());
    }

    @Test
    public void testLockItemToString() throws LockNotGrantedException, InterruptedException {
        final LockItem lockItem = this.lockClient.acquireLock(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1);
        final Pattern p = Pattern.compile("LockItem\\{Partition Key=testKey1, Sort Key=Optional.empty, Owner Name=" + INTEGRATION_TESTER + ", Lookup Time=\\d+, Lease Duration=3000, "
            + "Record Version Number=\\w+-\\w+-\\w+-\\w+-\\w+, Delete On Close=true, Is Released=false\\}");
        assertTrue(p.matcher(lockItem.toString()).matches());
    }

    @Test
    public void testLockItemHasSessionMonitor() throws InterruptedException, IOException {
        final LockItem item = this.getShortLeaseLockWithSessionMonitor(30L, null);
        assertTrue(item.hasSessionMonitor());
        item.close();
    }

    @Test
    public void testLockItemDoesNotHaveSessionMonitor() throws InterruptedException, IOException {
        final LockItem item = this.getShortLeaseLock();
        assertFalse(item.hasSessionMonitor());
        item.close();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSafeTimeLessThanHeartbeat() throws InterruptedException, IOException {
        final long badDangerZoneTimeMillis = 10L; // must be greater than heartbeat frequency (which is 10 millis)
        this.getShortLeaseLockWithSessionMonitor(badDangerZoneTimeMillis, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSafeTimeMoreThanLeaseDuration() throws InterruptedException, IOException {
        final long badDangerZoneTimeMillis = Long.MAX_VALUE; // must be less than leaseDuration
        this.getShortLeaseLockWithSessionMonitor(badDangerZoneTimeMillis, null);
    }

    @Test(expected = NullPointerException.class)
    public void testTimeUnitNotSetInAcquireLockOptionsWithSessionMonitor() throws InterruptedException, IOException {
        final AcquireLockOptions options = AcquireLockOptions.builder("testKey1").withSessionMonitor(SHORT_LEASE_DUR / 2, Optional.empty()).build();
        this.shortLeaseLockClient.acquireLock(options);
    }

    @Test(expected = SessionMonitorNotSetException.class)
    public void testFiringCallbackWithoutSessionMonitor() throws InterruptedException, IOException {
        final LockItem item = this.getShortLeaseLock();
        try {
            // we haven't called setSessionMonitor
            // try running the callback anyway
            item.runSessionMonitor();
        } finally {
            item.close();
        }
    }

    @Test(expected = SessionMonitorNotSetException.class)
    public void testAboutToExpireWithoutSessionMonitor() throws InterruptedException, IOException {
        final LockItem item = this.getShortLeaseLock();
        try {
            // we haven't called setSessionMonitor
            // try asking if about to expire anyway
            item.amIAboutToExpire();
        } finally {
            item.close();
        }
    }

    @Test
    public void testSessionMonitorCallbackFiredOnNonHeartbeatingLock() throws InterruptedException, IOException {
        final Object sync = new Object();
        this.getShortLeaseLockWithSessionMonitor(SHORT_LEASE_DUR / 2, notifyObj(sync));
        waitOn(sync, SHORT_LEASE_DUR);
        //Since there is no heartbeating thread, the lock doesn't get renewed: the callback should fire.
        //If the callback is not run in a timely manner, waitOn() will throw an AssertionError
    }

    @Test
    public void testShortLeaseNoHeartbeatCallbackFired() throws InterruptedException, IOException {
        this.testDangerZoneCallback(200L, 0, 15L, 170L);
    }

    @Test
    public void testLongLeaseHeartbeatCallbackFired() throws InterruptedException, IOException {
        this.testDangerZoneCallback(6000L, 5, 100L, 3500L);
    }

    @Test
    public void testHeartbeatAfterSafetyTimeout() throws InterruptedException, IOException {
        this.testDangerZoneCallback(100L, 0, 60L, 80L);
    }

    @Test
    public void testCallbackNotCalledOnClosingClient() throws InterruptedException, IOException {
        final long heartbeatFreq = 100L;
        final AmazonDynamoDBLockClient heartClient = Mockito.spy(new AmazonDynamoDBLockClient(
            AmazonDynamoDBLockClientOptions.builder(this.dynamoDBMock, TABLE_NAME).withOwnerName(LOCALHOST).withLeaseDuration(3 * heartbeatFreq).withHeartbeatPeriod(heartbeatFreq)
                .withTimeUnit(TimeUnit.MILLISECONDS).withCreateHeartbeatBackgroundThread(true).build()));
        final AtomicInteger integer = new AtomicInteger(0);
        final Runnable intSetter = () -> integer.set(1);
        final AcquireLockOptions options = stdSessionMonitorOptions(2 * heartbeatFreq, intSetter);
        final LockItem item = heartClient.acquireLock(options);
        heartClient.close();
        Thread.sleep(item.getLeaseDuration());
        assertEquals(0, integer.get());
    }

    @Test
    public void testHeartbeatAllLocks() throws InterruptedException, IOException {
        final long heartbeatFreq = 100L;
        final AmazonDynamoDBLockClient heartClient = Mockito.spy(new AmazonDynamoDBLockClient(
            AmazonDynamoDBLockClientOptions.builder(this.dynamoDBMock, TABLE_NAME).withOwnerName(LOCALHOST).withLeaseDuration(3 * heartbeatFreq).withHeartbeatPeriod(heartbeatFreq)
                .withTimeUnit(TimeUnit.MILLISECONDS).withCreateHeartbeatBackgroundThread(false).build()));

        final LockItem lock1 = heartClient.acquireLock(AcquireLockOptions.builder("lock1").build());
        final LockItem lock2 = heartClient.acquireLock(AcquireLockOptions.builder("lock2").build());
        final LockItem lock3 = heartClient.acquireLock(AcquireLockOptions.builder("lock3").build());

        Mockito.doThrow(new RuntimeException("lock1")).when(heartClient).sendHeartbeat(lock1);
        Mockito.doNothing().when(heartClient).sendHeartbeat(lock2);
        Mockito.doThrow(new RuntimeException("lock3")).when(heartClient).sendHeartbeat(lock3);

        final Thread t = new Thread(heartClient);
        t.start();

        Thread.sleep(heartbeatFreq * 5);
        t.interrupt();
        t.join(heartbeatFreq * 5);

        Mockito.verify(heartClient, Mockito.atLeast(4)).sendHeartbeat(lock1);
        Mockito.verify(heartClient, Mockito.atLeast(4)).sendHeartbeat(lock2);
        Mockito.verify(heartClient, Mockito.atLeast(4)).sendHeartbeat(lock3);

        heartClient.close();
    }

    @Test(expected = LockNotGrantedException.class)
    public void testAcquireLockOnClientException() throws InterruptedException, IOException {
        final String lockName = "lock1";
        final long heartbeatFreq = 100L;
        final long refreshPeriod = 200L;
        final int expectedAttempts = 3;
        final long additionalWaitTime = expectedAttempts * refreshPeriod;

        final AmazonDynamoDBLockClient testLockClient = Mockito.spy(new AmazonDynamoDBLockClient(
            AmazonDynamoDBLockClientOptions.builder(this.dynamoDBMock, TABLE_NAME).withOwnerName(LOCALHOST).withLeaseDuration(expectedAttempts * heartbeatFreq).withHeartbeatPeriod(heartbeatFreq)
                .withTimeUnit(TimeUnit.MILLISECONDS).withCreateHeartbeatBackgroundThread(false).build()));

        Mockito.doThrow(new AmazonClientException("Client exception acquiring lock")).when(testLockClient).getLockFromDynamoDB(any(GetLockOptions.class));
        try {
            testLockClient.acquireLock(
                AcquireLockOptions.builder(lockName).withRefreshPeriod(refreshPeriod).withAdditionalTimeToWaitForLock(additionalWaitTime).withTimeUnit(TimeUnit.MILLISECONDS)
                    .build());
        } finally {
            Mockito.verify(testLockClient, Mockito.atLeast(expectedAttempts + 1)).getLockFromDynamoDB(any(GetLockOptions.class));
            testLockClient.close();
        }
    }

    @Test
    public void testSetRequestLevelMetricCollector() throws InterruptedException, IOException {
        final AmazonDynamoDB dynamoDB = Mockito.mock(AmazonDynamoDB.class);
        when(dynamoDB.getItem(any(GetItemRequest.class))).thenReturn(new GetItemResult().withItem(null));
        when(dynamoDB.putItem(any(PutItemRequest.class))).thenReturn(Mockito.mock(PutItemResult.class));

        final AmazonDynamoDBLockClientOptions lockClientOptions =
            AmazonDynamoDBLockClientOptions.builder(dynamoDB, TABLE_NAME).withOwnerName("ownerName0").withTimeUnit(TimeUnit.SECONDS).withSortKeyName("rangeKeyName").build();

        final ArgumentCaptor<GetItemRequest> getRequestCaptor = ArgumentCaptor.forClass(GetItemRequest.class);
        final ArgumentCaptor<PutItemRequest> putRequestCaptor = ArgumentCaptor.forClass(PutItemRequest.class);
        final RequestMetricCollector requestMetricCollector = Mockito.mock(RequestMetricCollector.class);

        final AcquireLockOptions acquireLockOptions =
            AcquireLockOptions.builder("key0").withSortKey("rangeKey0").withDeleteLockOnRelease(false).withRequestMetricCollector(requestMetricCollector).build();

        try (AmazonDynamoDBLockClient testLockClient = new AmazonDynamoDBLockClient(lockClientOptions)) {
            testLockClient.acquireLock(acquireLockOptions);
        } finally {
            verify(dynamoDB).getItem(getRequestCaptor.capture());
            verify(dynamoDB).putItem(putRequestCaptor.capture());
        }

        final GetItemRequest getRequest = getRequestCaptor.getValue();
        final PutItemRequest putRequest = putRequestCaptor.getValue();
        assertEquals(requestMetricCollector, getRequest.getRequestMetricCollector());
        assertEquals(requestMetricCollector, putRequest.getRequestMetricCollector());
    }

    private LockItem getShortLeaseLock() throws InterruptedException {
        return this.shortLeaseLockClient.acquireLock(ACQUIRE_LOCK_OPTIONS_TEST_KEY_1);
    }

    private static AcquireLockOptions stdSessionMonitorOptions(final long safeTimeMillis, final Runnable callback) {
        return AcquireLockOptions.builder("testKey1").withSessionMonitor(safeTimeMillis, Optional.ofNullable(callback)).withTimeUnit(TimeUnit.MILLISECONDS).build();
    }

    private LockItem getShortLeaseLockWithSessionMonitor(final long safeTimeMillis, final Runnable callback) throws InterruptedException {
        return this.shortLeaseLockClient.acquireLock(stdSessionMonitorOptions(safeTimeMillis, callback));
    }

    private static void waitOn(final Object obj, final long timeoutMillis) {
        try {
            synchronized (obj) {
                obj.wait(timeoutMillis);
            }
        } catch (final InterruptedException e) {
            throw new AssertionError("Timed out", e);
        }
    }

    private static Runnable notifyObj(final Object obj) {
        return () -> {
            synchronized (obj) {
                obj.notify();
            }
        };
    }

    private void testDangerZoneCallback(final long leaseDurationMillis, final int nHeartbeats, final long heartbeatFrequencyMillis, final long safeTimeWithoutHeartbeatMillis)
        throws InterruptedException, IOException {

        // Set up a client with proper lease duration and heartbeat frequency
        // (no heartbeating thread)
        final AmazonDynamoDBLockClient dbClient = new AmazonDynamoDBLockClient(
            AmazonDynamoDBLockClientOptions.builder(this.dynamoDBMock, TABLE_NAME).withOwnerName(INTEGRATION_TESTER).withLeaseDuration(leaseDurationMillis).withHeartbeatPeriod(heartbeatFrequencyMillis)
                .withTimeUnit(TimeUnit.MILLISECONDS).withCreateHeartbeatBackgroundThread(false).build());

        // heartbeatCount will measure how many heartbeats we actually get (and
        // serve as a synchronization point)
        final AtomicInteger heartbeatCount = new AtomicInteger();

        try {
            // Acquire the lock, set a session monitor to call
            // heartbeatCount.notify() when the lock's lease goes into the
            // danger zone.
            final AcquireLockOptions options = stdSessionMonitorOptions(safeTimeWithoutHeartbeatMillis, notifyObj(heartbeatCount));
            final LockItem item = dbClient.acquireLock(options);

            // Start heartbeating on the lock (runs in the background).
            heartbeatNTimes(item, heartbeatCount, nHeartbeats, heartbeatFrequencyMillis);

            // Wait for heartbeatCount.notify()
            final long estimatedLeaseLifetimeMillis = nHeartbeats * heartbeatFrequencyMillis + leaseDurationMillis;
            waitOn(heartbeatCount, estimatedLeaseLifetimeMillis);

            // Lock should be in danger zone, not expired
            assertFalse(item.isExpired());
            assertTrue(item.amIAboutToExpire());

            // We should have heartbeated the proper number of times.
            assertEquals(nHeartbeats, heartbeatCount.get());

        } finally {
            dbClient.close();
        }
    }

    /**
     * For a given lock, this method spawns a new thread which heartbeats the
     * lock a specified number of times.
     *
     * @param item                the lock to heartbeat
     * @param heartbeatCount      a dummy variable that is passed to count the number of
     *                            heartbeats
     * @param nHeartbeats         the desired number of times to heartbeat the lock
     * @param heartbeatFreqMillis the period of time that between heartbeats
     */
    private static void heartbeatNTimes(final LockItem item, final AtomicInteger heartbeatCount, final int nHeartbeats, final long heartbeatFreqMillis) {
        final Thread worker = new Thread(() -> {
            for (heartbeatCount.set(0); heartbeatCount.get() < nHeartbeats; heartbeatCount.incrementAndGet()) {
                final long startTimeMillis = LockClientUtils.INSTANCE.millisecondTime();
                item.sendHeartBeat();
                final long timeSpentWorkingMillis = LockClientUtils.INSTANCE.millisecondTime() - startTimeMillis;
                final long timeLeftToSleepMillis = Math.max(0L, heartbeatFreqMillis - timeSpentWorkingMillis);
                try {
                    Thread.sleep(timeLeftToSleepMillis);
                } catch (final InterruptedException e) {
                    throw new AssertionError("Thread interrupted", e);
                }
            }
        });
        worker.start();
    }
}
