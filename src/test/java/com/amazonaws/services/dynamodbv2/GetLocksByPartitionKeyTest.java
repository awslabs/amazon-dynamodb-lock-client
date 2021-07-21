package com.amazonaws.services.dynamodbv2;

import com.amazonaws.services.dynamodbv2.model.LockNotGrantedException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.Test;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GetLocksByPartitionKeyTest extends InMemoryLockClientTester {
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
  public void getLocksFromDynamoDB_whenNoLocks_shouldReturnEmpty() throws LockNotGrantedException {
    final boolean deleteOnRelease = false;
    final List<LockItem>
        locksWithPartitionKey = this.lockClientForRangeKeyTable.getLocksByPartitionKey("test", deleteOnRelease).collect(toList());

    assertEquals(Collections.emptyList(), locksWithPartitionKey);
  }

  @Test
  public void getLocksFromDynamoDB_whenSingleLock_shouldReturnSingleLock() throws LockNotGrantedException, InterruptedException {
    final String partitionKey = "Test 1";
    final String sortKey = "1";
    final AcquireLockOptions options = AcquireLockOptions.builder(partitionKey)
        .withSortKey(sortKey).withData(ByteBuffer.wrap(TEST_DATA.getBytes())).withDeleteLockOnRelease(true).build();

    final LockItem singleLock = this.lockClientForRangeKeyTable.acquireLock(options);

    final boolean deleteOnRelease = false;
    List<LockItem> locksWithParitionKey = this.lockClientForRangeKeyTable.getLocksByPartitionKey(partitionKey, deleteOnRelease).collect(toList());

    assertEquals(1, locksWithParitionKey.size());

    final LockItem retrievedLock = locksWithParitionKey.get(0);
    assertEquals(singleLock.getPartitionKey(), retrievedLock.getPartitionKey());
    assertEquals(singleLock.getSortKey(), retrievedLock.getSortKey());
    assertTrue(
        Arrays.equals(getBytes(singleLock.getData().get()), getBytes(retrievedLock.getData().get())));
    assertEquals(singleLock.getRecordVersionNumber(), retrievedLock.getRecordVersionNumber());

    this.lockClientForRangeKeyTable.getLock(partitionKey, Optional.of(sortKey)).get().close();

    locksWithParitionKey = this.lockClientForRangeKeyTable.getLocksByPartitionKey(partitionKey, deleteOnRelease).collect(toList());
    assertEquals(Collections.emptyList(), locksWithParitionKey);
  }

  @Test
  public void getLocksFromDynamoDB_whenMultipleLocks_shouldReturnMultipleLocks() throws LockNotGrantedException, InterruptedException, IOException {
    final String partitionKey = "Test 1";
    final AcquireLockOptions options1 = AcquireLockOptions.builder(partitionKey)
        .withSortKey("1").withData(ByteBuffer.wrap(TEST_DATA.getBytes())).withDeleteLockOnRelease(true).build();
    final LockItem firstLock = this.lockClientForRangeKeyTable.acquireLock(options1);

    final AcquireLockOptions options2 = AcquireLockOptions.builder(partitionKey)
        .withSortKey("2").withData(ByteBuffer.wrap((TEST_DATA + "differentBytes").getBytes())).withDeleteLockOnRelease(true).build();
    final LockItem secondLock = this.lockClientForRangeKeyTable.acquireLock(options2);

    final boolean deleteOnRelease = false;
    List<LockItem> allLocksFromDynamoDB = this.lockClientForRangeKeyTable.getLocksByPartitionKey(partitionKey, deleteOnRelease).collect(toList());

    assertEquals(2, allLocksFromDynamoDB.size());

    final Map<String, LockItem> lockItemsByKey = toLockItemsByKey(allLocksFromDynamoDB);

    LockItem retrievedLock = lockItemsByKey.get(getLockKey(firstLock));
    assertEquals(firstLock.getPartitionKey(), retrievedLock.getPartitionKey());
    assertEquals(firstLock.getSortKey(), retrievedLock.getSortKey());
    assertTrue(Arrays.equals(getBytes(firstLock.getData().get()), getBytes(retrievedLock.getData().get())));
    assertEquals(firstLock.getRecordVersionNumber(), retrievedLock.getRecordVersionNumber());

    retrievedLock = lockItemsByKey.get(getLockKey(secondLock));
    assertEquals(secondLock.getPartitionKey(), retrievedLock.getPartitionKey());
    assertEquals(secondLock.getSortKey(), retrievedLock.getSortKey());
    assertTrue(Arrays.equals(getBytes(secondLock.getData().get()), getBytes(retrievedLock.getData().get())));
    assertEquals(secondLock.getRecordVersionNumber(), retrievedLock.getRecordVersionNumber());

    firstLock.close();

    allLocksFromDynamoDB = this.lockClientForRangeKeyTable.getLocksByPartitionKey(partitionKey, deleteOnRelease).collect(toList());

    assertEquals(1, allLocksFromDynamoDB.size());
    retrievedLock = allLocksFromDynamoDB.get(0);
    assertEquals(secondLock.getPartitionKey(), retrievedLock.getPartitionKey());
    assertEquals(secondLock.getSortKey(), retrievedLock.getSortKey());
    assertTrue(Arrays.equals(getBytes(secondLock.getData().get()), getBytes(retrievedLock.getData().get())));
    assertEquals(secondLock.getRecordVersionNumber(), retrievedLock.getRecordVersionNumber());

    secondLock.close();

    allLocksFromDynamoDB = this.lockClientForRangeKeyTable.getLocksByPartitionKey(partitionKey, deleteOnRelease).collect(toList());
    assertEquals(Collections.emptyList(), allLocksFromDynamoDB);
  }

  @Test
  public void getLocksFromDynamoDB_whenMultiplePages_shouldReturnMultiplePages() throws LockNotGrantedException, InterruptedException, IOException {
    // Scan is paginated.
    // Scans with items that are larger than 1MB are paginated
    // and must be retrieved by performing multiple scans.
    // Make sure the client handles pagination correctly.
    // See
    // http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/QueryAndScan.html#Pagination
    final String partitionKey = "Test 1";
    int numBytesOfData = 0;
    final byte[] data = new byte[(DYNAMODB_MAX_ITEM_SIZE_IN_BYTES * 9) / 10];
    final long randomSeed = System.currentTimeMillis();
    System.out.println("Random seed is " + randomSeed);
    final Map<String, LockItem> acquiredLockItemsByKey = new HashMap<>();
    while (numBytesOfData < DYNAMODB_MAX_PAGE_SIZE_IN_BYTES) {
      SECURE_RANDOM.nextBytes(data);
      int size = acquiredLockItemsByKey.size();
      final AcquireLockOptions options =
          AcquireLockOptions.builder(partitionKey)
              .withSortKey(Integer.toString(size))
              .withData(ByteBuffer.wrap(data)).withDeleteLockOnRelease(true).build();
      final LockItem acquiredLock = this.lockClientForRangeKeyTable.acquireLock(options);

      acquiredLockItemsByKey.put(getLockKey(acquiredLock), acquiredLock);
      numBytesOfData += acquiredLock.getData().get().array().length;
    }

    final boolean deleteOnRelease = false;
    List<LockItem> locksWithPartitionKey = this.lockClientForRangeKeyTable.getLocksByPartitionKey(partitionKey, deleteOnRelease).collect(toList());

    assertEquals(acquiredLockItemsByKey.size(), locksWithPartitionKey.size());

    final Map<String, LockItem> lockItemsByKey = toLockItemsByKey(locksWithPartitionKey);

    assertEquals(acquiredLockItemsByKey.keySet(), lockItemsByKey.keySet());

    for (final LockItem acquiredLock : acquiredLockItemsByKey.values()) {
      acquiredLock.close();
    }

    locksWithPartitionKey = this.lockClientForRangeKeyTable.getLocksByPartitionKey(partitionKey, deleteOnRelease).collect(toList());
    assertEquals(Collections.emptyList(), locksWithPartitionKey);
  }

  private static Map<String, LockItem> toLockItemsByKey(final List<LockItem> allLocksFromDynamoDB) {
    final Map<String, LockItem> locksByKey = new HashMap<>();
    for (final LockItem lockItem : allLocksFromDynamoDB) {
      locksByKey.put(getLockKey(lockItem), lockItem);
    }

    return locksByKey;
  }

  private static String getLockKey(LockItem lockItem) {
    return lockItem.getPartitionKey() + lockItem.getSortKey().orElse("");
  }

}
