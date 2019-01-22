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

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.Test;

import com.amazonaws.services.dynamodbv2.model.LockNotGrantedException;

/**
 * Tests the features of the lock client that involve getting all the locks
 * from DynamoDB.
 */
public class GetAllLocksTests extends InMemoryLockClientTester {
    /**
     * <p>1 MB</p>
     * <a href="http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/QueryAndScan.html#Pagination">
     * Dynamo DB Max Page Size Documentation
     * </a>
     */
    private static final int DYNAMODB_MAX_PAGE_SIZE_IN_BYTES = 1 << 20;

    /**
     * <p>400 KB</p>
     * <a href="http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithItems.html#WorkingWithItems.BatchOperations">
     * Dynamo DB Max Item Size Documentation
     * </a>
     */
    private static final int DYNAMODB_MAX_ITEM_SIZE_IN_BYTES = 400 * (1 << 10);

    @Test
    public void testGetAllLocksFromDynamoDBNoLocks() throws LockNotGrantedException, InterruptedException, IOException {
        final boolean deleteOnRelease = false;
        final List<LockItem> allLocksFromDynamoDB = this.lockClient.getAllLocksFromDynamoDB(deleteOnRelease).collect(toList());

        assertEquals(Collections.emptyList(), allLocksFromDynamoDB);
    }

    @Test
    public void testGetAllLocksFromDynamoDBSingleLock() throws LockNotGrantedException, InterruptedException, IOException {
        final AcquireLockOptions options = AcquireLockOptions.builder("Test 1").withData(ByteBuffer.wrap(TEST_DATA.getBytes())).withDeleteLockOnRelease(true).build();

        final LockItem singleLock = this.lockClient.acquireLock(options);

        final boolean deleteOnRelease = false;
        List<LockItem> allLocksFromDynamoDB = this.lockClient.getAllLocksFromDynamoDB(deleteOnRelease).collect(toList());

        assertEquals(1, allLocksFromDynamoDB.size());

        final LockItem retrievedLock = allLocksFromDynamoDB.get(0);
        assertEquals(singleLock.getPartitionKey(), retrievedLock.getPartitionKey());
        assertTrue(Arrays.equals(getBytes(singleLock.getData().get()), getBytes(retrievedLock.getData().get())));
        assertEquals(singleLock.getRecordVersionNumber(), retrievedLock.getRecordVersionNumber());

        this.lockClient.getLock("Test 1", Optional.empty()).get().close();

        allLocksFromDynamoDB = this.lockClient.getAllLocksFromDynamoDB(deleteOnRelease).collect(toList());
        assertEquals(Collections.emptyList(), allLocksFromDynamoDB);
    }

    @Test
    public void testGetAllLocksFromDynamoDBMultipleLocks() throws LockNotGrantedException, InterruptedException, IOException {
        final AcquireLockOptions options1 = AcquireLockOptions.builder("Test 1").withData(ByteBuffer.wrap(TEST_DATA.getBytes())).withDeleteLockOnRelease(true).build();
        final LockItem firstLock = this.lockClient.acquireLock(options1);

        final AcquireLockOptions options2 =
            AcquireLockOptions.builder("Test 2").withData(ByteBuffer.wrap((TEST_DATA + "differentBytes").getBytes())).withDeleteLockOnRelease(true).build();
        final LockItem secondLock = this.lockClient.acquireLock(options2);

        final boolean deleteOnRelease = false;
        List<LockItem> allLocksFromDynamoDB = this.lockClient.getAllLocksFromDynamoDB(deleteOnRelease).collect(toList());

        assertEquals(2, allLocksFromDynamoDB.size());

        final Map<String, LockItem> lockItemsByKey = toLockItemsByKey(allLocksFromDynamoDB);

        LockItem retrievedLock = lockItemsByKey.get(firstLock.getPartitionKey());
        assertEquals(firstLock.getPartitionKey(), retrievedLock.getPartitionKey());
        assertTrue(Arrays.equals(getBytes(firstLock.getData().get()), getBytes(retrievedLock.getData().get())));
        assertEquals(firstLock.getRecordVersionNumber(), retrievedLock.getRecordVersionNumber());

        retrievedLock = lockItemsByKey.get(secondLock.getPartitionKey());
        assertEquals(secondLock.getPartitionKey(), retrievedLock.getPartitionKey());
        assertTrue(Arrays.equals(getBytes(secondLock.getData().get()), getBytes(retrievedLock.getData().get())));
        assertEquals(secondLock.getRecordVersionNumber(), retrievedLock.getRecordVersionNumber());

        firstLock.close();

        allLocksFromDynamoDB = this.lockClient.getAllLocksFromDynamoDB(deleteOnRelease).collect(toList());

        assertEquals(1, allLocksFromDynamoDB.size());
        retrievedLock = allLocksFromDynamoDB.get(0);
        assertEquals(secondLock.getPartitionKey(), retrievedLock.getPartitionKey());
        assertTrue(Arrays.equals(getBytes(secondLock.getData().get()), getBytes(retrievedLock.getData().get())));
        assertEquals(secondLock.getRecordVersionNumber(), retrievedLock.getRecordVersionNumber());

        secondLock.close();

        allLocksFromDynamoDB = this.lockClient.getAllLocksFromDynamoDB(deleteOnRelease).collect(toList());
        assertEquals(Collections.emptyList(), allLocksFromDynamoDB);
    }

    @Test
    public void testGetAllLocksFromDynamoDBMultiplePagedResultSet() throws LockNotGrantedException, InterruptedException, IOException {
        // Scan is paginated.
        // Scans with items that are larger than 1MB are paginated
        // and must be retrieved by performing multiple scans.
        // Make sure the client handles pagination correctly.
        // See
        // http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/QueryAndScan.html#Pagination
        int numBytesOfData = 0;
        final byte[] data = new byte[(DYNAMODB_MAX_ITEM_SIZE_IN_BYTES * 9) / 10];
        final long randomSeed = System.currentTimeMillis();
        System.out.println("Random seed is " + randomSeed);
        final Map<String, LockItem> acquiredLockItemsByKey = new HashMap<>();
        while (numBytesOfData < DYNAMODB_MAX_PAGE_SIZE_IN_BYTES) {
            SECURE_RANDOM.nextBytes(data);
            final AcquireLockOptions options =
                AcquireLockOptions.builder(Integer.toString(acquiredLockItemsByKey.size())).withData(ByteBuffer.wrap(data)).withDeleteLockOnRelease(true).build();
            final LockItem acquiredLock = this.lockClient.acquireLock(options);

            acquiredLockItemsByKey.put(acquiredLock.getPartitionKey(), acquiredLock);
            numBytesOfData += acquiredLock.getData().get().array().length;
        }

        final boolean deleteOnRelease = false;
        List<LockItem> allLocksFromDynamoDB = this.lockClient.getAllLocksFromDynamoDB(deleteOnRelease).collect(toList());

        assertEquals(acquiredLockItemsByKey.size(), allLocksFromDynamoDB.size());

        final Map<String, LockItem> lockItemsByKey = toLockItemsByKey(allLocksFromDynamoDB);

        assertEquals(acquiredLockItemsByKey.keySet(), lockItemsByKey.keySet());

        for (final LockItem acquiredLock : acquiredLockItemsByKey.values()) {
            acquiredLock.close();
        }

        allLocksFromDynamoDB = this.lockClient.getAllLocksFromDynamoDB(deleteOnRelease).collect(toList());
        assertEquals(Collections.emptyList(), allLocksFromDynamoDB);
    }

    private static Map<String, LockItem> toLockItemsByKey(final List<LockItem> allLocksFromDynamoDB) {
        final Map<String, LockItem> locksByKey = new HashMap<>();
        for (final LockItem lockItem : allLocksFromDynamoDB) {
            locksByKey.put(lockItem.getPartitionKey(), lockItem);
        }

        return locksByKey;
    }

}
