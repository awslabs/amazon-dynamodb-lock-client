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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpStatus;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.metrics.RequestMetricCollector;
import com.amazonaws.annotation.ThreadSafe;
import com.amazonaws.services.dynamodbv2.GetLockOptions.GetLockOptionsBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.LockCurrentlyUnavailableException;
import com.amazonaws.services.dynamodbv2.model.LockNotGrantedException;
import com.amazonaws.services.dynamodbv2.model.LockTableDoesNotExistException;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.TableStatus;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.util.LockClientUtils;

/**
 * <p>
 * Provides a simple library for using DynamoDB's consistent read/write feature to use it for managing distributed locks.
 * </p>
 * <p>
 * In order to use this library, the client must create a table in DynamoDB, although the library provides a convenience method
 * for creating that table (createLockTableInDynamoDB.)
 * </p>
 * <p>
 * Here is some example code for how to use the lock client for leader election to work on a resource called "database-3" (it
 * assumes you already have a DynamoDB table named lockTable, which can be created with the static
 * {@code createLockTableInDynamoDB} helper method):
 * </p>
 * <pre>
 * {@code
 *  AmazonDynamoDBLockClient lockClient = new AmazonDynamoDBLockClient(
 *      AmazonDynamoDBLockClientOptions.builder(dynamoDBClient, "lockTable").build();
 *  try {
 *      // Attempt to acquire the lock indefinitely, polling DynamoDB every 2 seconds for the lock
 *      LockItem lockItem = lockClient.acquireLock(
 *          AcquireLockOptions.builder("database-3")
 *              .withRefreshPeriod(120L)
 *              .withAdditionalTimeToWaitForLock(Long.MAX_VALUE / 2L)
 *              .withTimeUnit(TimeUnit.MILLISECONDS)
 *              .build());
 *      if (!lockItem.isExpired()) {
 *          // do business logic, you can call lockItem.isExpired() to periodically check to make sure you still have the lock
 *          // the background thread will keep the lock valid for you by sending heartbeats (default is every 5 seconds)
 *      }
 *  } catch (LockNotGrantedException x) {
 *      // Should only be thrown if the lock could not be acquired for Long.MAX_VALUE / 2L milliseconds.
 *  }
 * }
 * </pre>
 * <p>
 * Here is an example that involves a bunch of workers getting customer IDs from a queue, taking a lock on that Customer ID, then
 * releasing that lock when complete:
 * </p>
 * <pre>
 * {@code
 *  AmazonDynamoDBLockClient lockClient = new AmazonDynamoDBLockClient(
 *      AmazonDynamoDBLockClient.builder(dynamoDBClient, "lockTable").build();
 *  while (true) {
 *      // Somehow find out about what work needs to be done
 *      String customerID = getCustomerIDFromQueue();
 *
 *     try {
 *          // Don't try indefinitely -- if someone else has a lock on this Customer ID, just move onto the next customer
 *          // (note that, if there is a lock on this customer ID, this method will still wait at least 20 seconds in order to be
 *          // able to determine
 *          // if that lock is stale)
 *          LockItem lockItem = lockClient.acquireLock(AcquireLockOptions.builder(customerID).build());
 *          if (!lockItem.isExpired()) {
 *              // Perform operation on this customer
 *          }
 *          lockItem.close();
 *      } catch (LockNotGrantedException x) {
 *          logger.info("We failed to acquire the lock for customer " + customerID, x);
 *      }
 *  }
 * }
 * </pre>
 *
 * @author <a href="mailto:slutsker@amazon.com">Sasha Slutsker</a>
 * @author <a href="mailto:amcp@amazon.com">Alexander Patrikalakis</a>
 */
@ThreadSafe
public class AmazonDynamoDBLockClient implements Runnable, Closeable {
    private static final Log logger = LogFactory.getLog(AmazonDynamoDBLockClient.class);
    private static final Set<TableStatus> availableStatuses;
    protected static final String SK_PATH_EXPRESSION_VARIABLE = "#sk";
    protected static final String PK_PATH_EXPRESSION_VARIABLE = "#pk";
    protected static final String NEW_RVN_VALUE_EXPRESSION_VARIABLE = ":newRvn";
    protected static final String LEASE_DURATION_PATH_VALUE_EXPRESSION_VARIABLE = "#ld";
    protected static final String LEASE_DURATION_VALUE_EXPRESSION_VARIABLE = ":ld";
    protected static final String RVN_PATH_EXPRESSION_VARIABLE = "#rvn";
    protected static final String RVN_VALUE_EXPRESSION_VARIABLE = ":rvn";
    protected static final String OWNER_NAME_PATH_EXPRESSION_VARIABLE = "#on";
    protected static final String OWNER_NAME_VALUE_EXPRESSION_VARIABLE = ":on";
    protected static final String DATA_PATH_EXPRESSION_VARIABLE = "#d";
    protected static final String DATA_VALUE_EXPRESSION_VARIABLE = ":d";
    protected static final String IS_RELEASED_PATH_EXPRESSION_VARIABLE = "#ir";
    protected static final String IS_RELEASED_VALUE_EXPRESSION_VARIABLE = ":ir";

    //attribute_not_exists(#pk)
    protected static final String ACQUIRE_LOCK_THAT_DOESNT_EXIST_PK_CONDITION = String.format(
        "attribute_not_exists(%s)",
        PK_PATH_EXPRESSION_VARIABLE);

    //attribute_not_exists(#pk) AND attribute_not_exists(#sk)
    protected static final String ACQUIRE_LOCK_THAT_DOESNT_EXIST_PK_SK_CONDITION = String.format(
            "attribute_not_exists(%s) AND attribute_not_exists(%s)",
            PK_PATH_EXPRESSION_VARIABLE, SK_PATH_EXPRESSION_VARIABLE);

    //attribute_exists(#pk) AND #ir = :ir
    protected static final String PK_EXISTS_AND_IS_RELEASED_CONDITION = String.format("attribute_exists(%s) AND %s = %s",
            PK_PATH_EXPRESSION_VARIABLE, IS_RELEASED_PATH_EXPRESSION_VARIABLE, IS_RELEASED_VALUE_EXPRESSION_VARIABLE);

    //attribute_exists(#pk) AND attribute_exists(#sk) AND #ir = :ir
    protected static final String PK_EXISTS_AND_SK_EXISTS_AND_IS_RELEASED_CONDITION = String.format(
            "attribute_exists(%s) AND attribute_exists(%s) AND %s = %s",
            PK_PATH_EXPRESSION_VARIABLE, SK_PATH_EXPRESSION_VARIABLE, IS_RELEASED_PATH_EXPRESSION_VARIABLE, IS_RELEASED_VALUE_EXPRESSION_VARIABLE);

    //attribute_exists(#pk) AND attribute_exists(#sk) AND #rvn = :rvn AND #ir = :ir
    protected static final String PK_EXISTS_AND_SK_EXISTS_AND_RVN_IS_THE_SAME_AND_IS_RELEASED_CONDITION = String.format(
            "attribute_exists(%s) AND attribute_exists(%s) AND %s = %s AND %s = %s",
            PK_PATH_EXPRESSION_VARIABLE, SK_PATH_EXPRESSION_VARIABLE, RVN_PATH_EXPRESSION_VARIABLE, RVN_VALUE_EXPRESSION_VARIABLE,
            IS_RELEASED_PATH_EXPRESSION_VARIABLE, IS_RELEASED_VALUE_EXPRESSION_VARIABLE);

    //attribute_exists(#pk) AND attribute_exists(#sk) AND #rvn = :rvn
    protected static final String PK_EXISTS_AND_SK_EXISTS_AND_RVN_IS_THE_SAME_CONDITION =
        String.format("attribute_exists(%s) AND attribute_exists(%s) AND %s = %s",
            PK_PATH_EXPRESSION_VARIABLE, SK_PATH_EXPRESSION_VARIABLE, RVN_PATH_EXPRESSION_VARIABLE, RVN_VALUE_EXPRESSION_VARIABLE);

    //(attribute_exists(#pk) AND attribute_exists(#sk) AND #rvn = :rvn) AND (attribute_not_exists(#if) OR #if = :if) AND #on = :on
    protected static final String PK_EXISTS_AND_SK_EXISTS_AND_OWNER_NAME_SAME_AND_RVN_SAME_CONDITION =
        String.format("%s AND %s = %s ",
                PK_EXISTS_AND_SK_EXISTS_AND_RVN_IS_THE_SAME_CONDITION, OWNER_NAME_PATH_EXPRESSION_VARIABLE, OWNER_NAME_VALUE_EXPRESSION_VARIABLE);

    //(attribute_exists(#pk) AND #rvn = :rvn AND #ir = :ir) AND (attribute_not_exists(#if) OR #if = :if)
    protected static final String PK_EXISTS_AND_RVN_IS_THE_SAME_AND_IS_RELEASED_CONDITION =
            String.format("(attribute_exists(%s) AND %s = %s AND %s = %s)",
                          PK_PATH_EXPRESSION_VARIABLE, RVN_PATH_EXPRESSION_VARIABLE, RVN_VALUE_EXPRESSION_VARIABLE,
                          IS_RELEASED_PATH_EXPRESSION_VARIABLE, IS_RELEASED_VALUE_EXPRESSION_VARIABLE);

    //attribute_exists(#pk) AND #rvn = :rvn AND (attribute_not_exists(#if) OR #if = :if)
    protected static final String PK_EXISTS_AND_RVN_IS_THE_SAME_CONDITION =
        String.format("attribute_exists(%s) AND %s = %s",
            PK_PATH_EXPRESSION_VARIABLE, RVN_PATH_EXPRESSION_VARIABLE, RVN_VALUE_EXPRESSION_VARIABLE);

    //attribute_exists(#pk) AND #rvn = :rvn AND (attribute_not_exists(#if) OR #if = :if) AND #on = :on
    protected static final String PK_EXISTS_AND_OWNER_NAME_SAME_AND_RVN_SAME_CONDITION =
        String.format("%s AND %s = %s",
            PK_EXISTS_AND_RVN_IS_THE_SAME_CONDITION, OWNER_NAME_PATH_EXPRESSION_VARIABLE, OWNER_NAME_VALUE_EXPRESSION_VARIABLE);

    protected static final String UPDATE_IS_RELEASED = String.format("SET %s = %s", IS_RELEASED_PATH_EXPRESSION_VARIABLE, IS_RELEASED_VALUE_EXPRESSION_VARIABLE);
    protected static final String UPDATE_IS_RELEASED_AND_DATA =
        String.format("%s, %s = %s", UPDATE_IS_RELEASED, DATA_PATH_EXPRESSION_VARIABLE, DATA_VALUE_EXPRESSION_VARIABLE);
    protected static final String UPDATE_LEASE_DURATION_AND_RVN = String.format(
        "SET %s = %s, %s = %s",
        LEASE_DURATION_PATH_VALUE_EXPRESSION_VARIABLE, LEASE_DURATION_VALUE_EXPRESSION_VARIABLE, RVN_PATH_EXPRESSION_VARIABLE, NEW_RVN_VALUE_EXPRESSION_VARIABLE);
    protected static final String UPDATE_LEASE_DURATION_AND_RVN_AND_REMOVE_DATA = String.format("%s REMOVE %s", UPDATE_LEASE_DURATION_AND_RVN, DATA_PATH_EXPRESSION_VARIABLE);
    protected static final String UPDATE_LEASE_DURATION_AND_RVN_AND_DATA = String.format("%s, %s = %s",
        UPDATE_LEASE_DURATION_AND_RVN, DATA_PATH_EXPRESSION_VARIABLE, DATA_VALUE_EXPRESSION_VARIABLE);
    protected static final String REMOVE_IS_RELEASED_UPDATE_EXPRESSION = String.format(" REMOVE %s ", IS_RELEASED_PATH_EXPRESSION_VARIABLE);

    static {
        availableStatuses = new HashSet<>();
        availableStatuses.add(TableStatus.ACTIVE);
        availableStatuses.add(TableStatus.UPDATING);
    }

    protected final AmazonDynamoDB dynamoDB;
    protected final String tableName;
    private final String partitionKeyName;
    private final Optional<String> sortKeyName;
    private final long leaseDurationInMilliseconds;
    private final long heartbeatPeriodInMilliseconds;
    private final boolean holdLockOnServiceUnavailable;
    private final String ownerName;
    private final ConcurrentHashMap<String, LockItem> locks;
    private final ConcurrentHashMap<String, Thread> sessionMonitors;
    private final Optional<Thread> backgroundThread;
    private final Function<String, ThreadFactory> namedThreadCreator;
    private volatile boolean shuttingDown = false;

    /* These are the keys that are stored in the DynamoDB table to keep track of the locks */
    protected static final String DATA = "data";
    protected static final String OWNER_NAME = "ownerName";
    protected static final String LEASE_DURATION = "leaseDuration";
    protected static final String RECORD_VERSION_NUMBER = "recordVersionNumber";
    protected static final String IS_RELEASED = "isReleased";
    protected static final String IS_RELEASED_VALUE = "1";
    protected static final AttributeValue IS_RELEASED_ATTRIBUTE_VALUE = new AttributeValue(IS_RELEASED_VALUE);
    protected static volatile AtomicInteger lockClientId = new AtomicInteger(0);
    protected static final Boolean IS_RELEASED_INDICATOR = true;
    /*
     * Used as a default buffer for how long extra to wait when querying DynamoDB for a lock in acquireLock (can be overriden by
     * specifying a timeout when calling acquireLock)
     */
    private static final long DEFAULT_BUFFER_MS = 1000;

    /**
     * Initializes an AmazonDynamoDBLockClient using the lock client options
     * specified in the AmazonDynamoDBLockClientOptions object.
     *
     * @param amazonDynamoDBLockClientOptions The options to use when initializing the client, i.e. the
     *                                        table name, sort key value, etc.
     */
    public AmazonDynamoDBLockClient(final AmazonDynamoDBLockClientOptions amazonDynamoDBLockClientOptions) {
        Objects.requireNonNull(amazonDynamoDBLockClientOptions.getDynamoDBClient(), "DynamoDB client object cannot be null");
        Objects.requireNonNull(amazonDynamoDBLockClientOptions.getTableName(), "Table name cannot be null");
        Objects.requireNonNull(amazonDynamoDBLockClientOptions.getOwnerName(), "Owner name cannot be null");
        Objects.requireNonNull(amazonDynamoDBLockClientOptions.getTimeUnit(), "Time unit cannot be null");
        Objects.requireNonNull(amazonDynamoDBLockClientOptions.getPartitionKeyName(), "Partition Key Name cannot be null");
        Objects.requireNonNull(amazonDynamoDBLockClientOptions.getSortKeyName(), "Sort Key Name cannot be null (use Optional.absent())");
        Objects.requireNonNull(amazonDynamoDBLockClientOptions.getNamedThreadCreator(), "Named thread creator cannot be null");
        this.dynamoDB = amazonDynamoDBLockClientOptions.getDynamoDBClient();
        this.tableName = amazonDynamoDBLockClientOptions.getTableName();
        this.locks = new ConcurrentHashMap<>();
        this.sessionMonitors = new ConcurrentHashMap<>();
        this.ownerName = amazonDynamoDBLockClientOptions.getOwnerName();
        this.leaseDurationInMilliseconds = amazonDynamoDBLockClientOptions.getTimeUnit().toMillis(amazonDynamoDBLockClientOptions.getLeaseDuration());
        this.heartbeatPeriodInMilliseconds = amazonDynamoDBLockClientOptions.getTimeUnit().toMillis(amazonDynamoDBLockClientOptions.getHeartbeatPeriod());
        this.partitionKeyName = amazonDynamoDBLockClientOptions.getPartitionKeyName();
        this.sortKeyName = amazonDynamoDBLockClientOptions.getSortKeyName();
        this.namedThreadCreator = amazonDynamoDBLockClientOptions.getNamedThreadCreator();
        this.holdLockOnServiceUnavailable = amazonDynamoDBLockClientOptions.getHoldLockOnServiceUnavailable();

        if (amazonDynamoDBLockClientOptions.getCreateHeartbeatBackgroundThread()) {
            if (this.leaseDurationInMilliseconds < 2 * this.heartbeatPeriodInMilliseconds) {
                throw new IllegalArgumentException("Heartbeat period must be no more than half the length of the Lease Duration, "
                    + "or locks might expire due to the heartbeat thread taking too long to update them (recommendation is to make it much greater, for example "
                    + "4+ times greater)");
            }
            this.backgroundThread = Optional.of(this.startBackgroundThread());
        } else {
            this.backgroundThread = Optional.empty();
        }
    }

    /**
     * Checks whether the lock table exists in DynamoDB.
     *
     * @return true if the table exists, false otherwise.
     */
    public boolean lockTableExists() {
        try {
            final DescribeTableResult result = this.dynamoDB.describeTable(new DescribeTableRequest().withTableName(this.tableName));
            return availableStatuses.contains(TableStatus.fromValue(result.getTable().getTableStatus()));
        } catch (final ResourceNotFoundException e) {
            // This exception indicates the table doesn't exist.
            return false;
        }
    }

    /**
     * Asserts that the lock table exists in DynamoDB. You can use this method
     * during application initialization to ensure that the lock client will be
     * usable. Since this is a no-arg assertion as opposed to a check that
     * returns a value, this method is also suitable as an init-method for a
     * Spring bean.
     *
     * @throws LockTableDoesNotExistException if the table doesn't exist.
     */
    public void assertLockTableExists() throws LockTableDoesNotExistException {
        boolean exists;
        try {
            exists = this.lockTableExists();
        } catch (final Exception e) {
            throw new LockTableDoesNotExistException("Lock table " + this.tableName + " does not exist", e);
        }
        if (!exists) {
            throw new LockTableDoesNotExistException("Lock table " + this.tableName + " does not exist");
        }
    }

    /**
     * Creates a DynamoDB table with the right schema for it to be used by this locking library. The table should be set up in advance,
     * because it takes a few minutes for DynamoDB to provision a new instance. Also, if the table already exists, this will throw an exception.
     * <p>
     * This method lets you specify a sort key to be used by the lock client. This sort key then needs to be specified in the
     * AmazonDynamoDBLockClientOptions when the lock client object is created.
     *
     * @param createDynamoDBTableOptions The options for the lock client
     */
    public static void createLockTableInDynamoDB(final CreateDynamoDBTableOptions createDynamoDBTableOptions) {
        Objects.requireNonNull(createDynamoDBTableOptions.getDynamoDBClient(), "DynamoDB client object cannot be null");
        Objects.requireNonNull(createDynamoDBTableOptions.getTableName(), "Table name cannot be null");
        Objects.requireNonNull(createDynamoDBTableOptions.getProvisionedThroughput(), "Provisioned throughput cannot be null");
        Objects.requireNonNull(createDynamoDBTableOptions.getPartitionKeyName(), "Hash Key Name cannot be null");
        Objects.requireNonNull(createDynamoDBTableOptions.getSortKeyName(), "Sort Key Name cannot be null");
        final KeySchemaElement partitionKeyElement = new KeySchemaElement();
        partitionKeyElement.setAttributeName(createDynamoDBTableOptions.getPartitionKeyName());
        partitionKeyElement.setKeyType(KeyType.HASH);

        final List<KeySchemaElement> keySchema = new ArrayList<>();
        keySchema.add(partitionKeyElement);

        final Collection<AttributeDefinition> attributeDefinitions = new ArrayList<>();
        attributeDefinitions.add(new AttributeDefinition().withAttributeName(createDynamoDBTableOptions.getPartitionKeyName()).withAttributeType(ScalarAttributeType.S));

        if (createDynamoDBTableOptions.getSortKeyName().isPresent()) {
            final KeySchemaElement sortKeyElement = new KeySchemaElement();
            sortKeyElement.setAttributeName(createDynamoDBTableOptions.getSortKeyName().get());
            sortKeyElement.setKeyType(KeyType.RANGE);
            keySchema.add(sortKeyElement);
            attributeDefinitions.add(new AttributeDefinition().withAttributeName(createDynamoDBTableOptions.getSortKeyName().get()).withAttributeType(ScalarAttributeType.S));
        }

        final CreateTableRequest createTableRequest = new CreateTableRequest(createDynamoDBTableOptions.getTableName(), keySchema);
        createTableRequest.setProvisionedThroughput(createDynamoDBTableOptions.getProvisionedThroughput());
        createTableRequest.setAttributeDefinitions(attributeDefinitions);

        if (createDynamoDBTableOptions.getRequestMetricCollector().isPresent()) {
            createTableRequest.setRequestMetricCollector(createDynamoDBTableOptions.getRequestMetricCollector().get());
        }

        createDynamoDBTableOptions.getDynamoDBClient().createTable(createTableRequest);
    }

    /**
     * <p>
     * Attempts to acquire a lock until it either acquires the lock, or a specified {@code additionalTimeToWaitForLock} is
     * reached. This method will poll DynamoDB based on the {@code refreshPeriod}. If it does not see the lock in DynamoDB, it
     * will immediately return the lock to the caller. If it does see the lock, it will note the lease expiration on the lock. If
     * the lock is deemed stale, (that is, there is no heartbeat on it for at least the length of its lease duration) then this
     * will acquire and return it. Otherwise, if it waits for as long as {@code additionalTimeToWaitForLock} without acquiring the
     * lock, then it will throw a {@code LockNotGrantedException}.
     * </p>
     * <p>
     * Note that this method will wait for at least as long as the {@code leaseDuration} in order to acquire a lock that already
     * exists. If the lock is not acquired in that time, it will wait an additional amount of time specified in
     * {@code additionalTimeToWaitForLock} before giving up.
     * </p>
     * <p>
     * See the defaults set when constructing a new {@code AcquireLockOptions} object for any fields that you do not set
     * explicitly.
     * </p>
     *
     * @param options A combination of optional arguments that may be passed in for acquiring the lock
     * @return the lock
     * @throws InterruptedException in case the Thread.sleep call was interrupted while waiting to refresh.
     */
    @SuppressWarnings("resource") // LockItem.close() does not need to be called until the lock is acquired, so we suppress the warning here.
    public LockItem acquireLock(final AcquireLockOptions options) throws LockNotGrantedException, InterruptedException {
        Objects.requireNonNull(options, "Cannot acquire lock when options is null");
        Objects.requireNonNull(options.getPartitionKey(), "Cannot acquire lock when key is null");

        final String key = options.getPartitionKey();
        final Optional<String> sortKey = options.getSortKey();

        if (options.getAdditionalAttributes().containsKey(this.partitionKeyName) || options.getAdditionalAttributes().containsKey(OWNER_NAME) || options
            .getAdditionalAttributes().containsKey(LEASE_DURATION) || options.getAdditionalAttributes().containsKey(RECORD_VERSION_NUMBER) || options
            .getAdditionalAttributes().containsKey(DATA) || this.sortKeyName.isPresent() && options.getAdditionalAttributes().containsKey(this.sortKeyName.get())) {
            throw new IllegalArgumentException(String
                .format("Additional attribute cannot be one of the following types: " + "%s, %s, %s, %s, %s", this.partitionKeyName, OWNER_NAME, LEASE_DURATION,
                    RECORD_VERSION_NUMBER, DATA));
        }

        long millisecondsToWait = DEFAULT_BUFFER_MS;
        if (options.getAdditionalTimeToWaitForLock() != null) {
            Objects.requireNonNull(options.getTimeUnit(), "timeUnit must not be null if additionalTimeToWaitForLock is non-null");
            millisecondsToWait = options.getTimeUnit().toMillis(options.getAdditionalTimeToWaitForLock());
        }

        long refreshPeriodInMilliseconds = DEFAULT_BUFFER_MS;
        if (options.getRefreshPeriod() != null) {
            Objects.requireNonNull(options.getTimeUnit(), "timeUnit must not be null if refreshPeriod is non-null");
            refreshPeriodInMilliseconds = options.getTimeUnit().toMillis(options.getRefreshPeriod());
        }

        final boolean deleteLockOnRelease = options.getDeleteLockOnRelease();
        final boolean replaceData = options.getReplaceData();

        final Optional<SessionMonitor> sessionMonitor = options.getSessionMonitor();
        if (sessionMonitor.isPresent()) {
            sessionMonitorArgsValidate(sessionMonitor.get().getSafeTimeMillis(), this.heartbeatPeriodInMilliseconds, this.leaseDurationInMilliseconds);
        }
        final long currentTimeMillis = LockClientUtils.INSTANCE.millisecondTime();

        /*
         * This is the lock we are trying to acquire. If it already exists, then we can try to steal it if it does not get updated
         * after its LEASE_DURATION expires.
         */
        LockItem lockTryingToBeAcquired = null;
        boolean alreadySleptOnceForOneLeasePeriod = false;

        final GetLockOptions getLockOptions = new GetLockOptions.GetLockOptionsBuilder(key).withSortKey(sortKey.orElse(null)).withDeleteLockOnRelease(deleteLockOnRelease)
            .withRequestMetricCollector(options.getRequestMetricCollector().orElse(null)).build();

        while (true) {
            try {
                try {
                    logger.trace("Call GetItem to see if the lock for " + partitionKeyName + " =" + key + ", " + this.sortKeyName + "=" + sortKey + " exists in the table");
                    final Optional<LockItem> existingLock = this.getLockFromDynamoDB(getLockOptions);

                    if (options.getAcquireOnlyIfLockAlreadyExists() && !existingLock.isPresent()) {
                        throw new LockNotGrantedException("Lock does not exist.");
                    }

                    if (options.shouldSkipBlockingWait() && existingLock.isPresent() && !existingLock.get().isExpired()) {
                        /*
                         * The lock is being held by some one and is still not expired. And the caller explicitly said not to perform a blocking wait;
                         * We will throw back a lock not grant exception, so that the caller can retry if needed.
                         */
                        throw new LockCurrentlyUnavailableException("The lock being requested is being held by another client.");
                    }

                    Optional<ByteBuffer> newLockData = Optional.empty();
                    if (replaceData) {
                        newLockData = options.getData();
                    } else if (existingLock.isPresent()) {
                        newLockData = existingLock.get().getData();
                    }

                    if (!newLockData.isPresent()) {
                        newLockData = options.getData(); // If there is no existing data, we write the input data to the lock.
                    }

                    final Map<String, AttributeValue> item = new HashMap<>();
                    item.putAll(options.getAdditionalAttributes());
                    item.put(this.partitionKeyName, new AttributeValue().withS(key));
                    item.put(OWNER_NAME, new AttributeValue().withS(this.ownerName));
                    item.put(LEASE_DURATION, new AttributeValue().withS(String.valueOf(this.leaseDurationInMilliseconds)));
                    final String recordVersionNumber = this.generateRecordVersionNumber();
                    item.put(RECORD_VERSION_NUMBER, new AttributeValue().withS(String.valueOf(recordVersionNumber)));
                    sortKeyName.ifPresent(sortKeyName -> item.put(sortKeyName, new AttributeValue().withS(sortKey.get())));
                    newLockData.ifPresent(byteBuffer -> item.put(DATA, new AttributeValue().withB(byteBuffer)));

                    //if the existing lock does not exist or exists and is released
                    if (!existingLock.isPresent() && !options.getAcquireOnlyIfLockAlreadyExists()) {
                        return upsertAndMonitorNewLock(options, key, sortKey, deleteLockOnRelease, sessionMonitor, newLockData,
                            item, recordVersionNumber);
                    } else if (existingLock.isPresent() && existingLock.get().isReleased()) {
                        return upsertAndMonitorReleasedLock(options, key, sortKey, deleteLockOnRelease, sessionMonitor, existingLock,
                            newLockData, item, recordVersionNumber);
                    }
                    // we know that we didnt enter the if block above because it returns at the end.
                    // we also know that the existingLock.isPresent() is true
                    if (lockTryingToBeAcquired == null) {
                        //this branch of logic only happens once, in the first iteration of the while loop
                        //lockTryingToBeAcquired only ever gets set to non-null values after this point.
                        //so it is impossible to get in this
                        /*
                         * Someone else has the lock, and they have the lock for LEASE_DURATION time. At this point, we need
                         * to wait at least LEASE_DURATION milliseconds before we can try to acquire the lock.
                         */
                        lockTryingToBeAcquired = existingLock.get();
                        if (!alreadySleptOnceForOneLeasePeriod) {
                            alreadySleptOnceForOneLeasePeriod = true;
                            millisecondsToWait += existingLock.get().getLeaseDuration();
                        }
                    } else {
                        if (lockTryingToBeAcquired.getRecordVersionNumber().equals(existingLock.get().getRecordVersionNumber())) {
                            /* If the version numbers match, then we can acquire the lock, assuming it has already expired */
                            if (lockTryingToBeAcquired.isExpired()) {
                                return upsertAndMonitorExpiredLock(options, key, sortKey, deleteLockOnRelease, sessionMonitor, existingLock, newLockData, item,
                                    recordVersionNumber);
                            }
                        } else {
                            /*
                             * If the version number changed since we last queried the lock, then we need to update
                             * lockTryingToBeAcquired as the lock has been refreshed since we last checked
                             */
                            lockTryingToBeAcquired = existingLock.get();
                        }
                    }
                } catch (final ConditionalCheckFailedException conditionalCheckFailedException) {
                    /* Someone else acquired the lock while we tried to do so, so we throw an exception */
                    logger.debug("Someone else acquired the lock", conditionalCheckFailedException);
                    throw new LockNotGrantedException("Could not acquire lock because someone else acquired it: ", conditionalCheckFailedException);
                } catch (ProvisionedThroughputExceededException provisionedThroughputExceededException) {
                    /* Request exceeded maximum allowed provisioned throughput for the table
                     * or for one or more global secondary indexes.
                     */
                    logger.debug("Maximum allowed provisioned throughput for the table exceeded", provisionedThroughputExceededException);
                    throw new LockNotGrantedException("Could not acquire lock because provisioned throughput for the table exceeded", provisionedThroughputExceededException);
                } catch (final AmazonClientException amazonClientException) {
                    /* This indicates that we were unable to successfully connect and make a service call to DDB. Often
                     * indicative of a network failure, such as a socket timeout. We retry if still within the time we
                     * can wait to acquire the lock.
                     */
                    logger.warn("Could not acquire lock because of a client side failure in talking to DDB", amazonClientException);
                }
            } catch (final LockNotGrantedException x) {
                if (LockClientUtils.INSTANCE.millisecondTime() - currentTimeMillis > millisecondsToWait) {
                    logger.debug("This client waited more than millisecondsToWait=" + millisecondsToWait
                        + " ms since the beginning of this acquire call.", x);
                    throw x;
                }
            }
            if (LockClientUtils.INSTANCE.millisecondTime() - currentTimeMillis > millisecondsToWait) {
                throw new LockNotGrantedException("Didn't acquire lock after sleeping for " + (LockClientUtils.INSTANCE.millisecondTime() - currentTimeMillis) + " milliseconds");
            }
            logger.trace("Sleeping for a refresh period of " + refreshPeriodInMilliseconds + " ms");
            Thread.sleep(refreshPeriodInMilliseconds);
        }
    }

    private LockItem upsertAndMonitorExpiredLock(AcquireLockOptions options, String key, Optional<String> sortKey, boolean deleteLockOnRelease,
        Optional<SessionMonitor> sessionMonitor, Optional<LockItem> existingLock, Optional<ByteBuffer> newLockData, Map<String, AttributeValue> item, String recordVersionNumber) {
        final String conditionalExpression;
        final Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        final boolean updateExistingLockRecord = options.getUpdateExistingLockRecord();
        expressionAttributeValues.put(RVN_VALUE_EXPRESSION_VARIABLE, new AttributeValue(existingLock.get().getRecordVersionNumber()));
        final Map<String, String> expressionAttributeNames = new HashMap<>();
        expressionAttributeNames.put(PK_PATH_EXPRESSION_VARIABLE, partitionKeyName);
        expressionAttributeNames.put(RVN_PATH_EXPRESSION_VARIABLE, RECORD_VERSION_NUMBER);
        if (this.sortKeyName.isPresent()) {
            //We do not check the owner here because the lock is expired and it is OK to overwrite the owner
            conditionalExpression = PK_EXISTS_AND_SK_EXISTS_AND_RVN_IS_THE_SAME_CONDITION;
            expressionAttributeNames.put(SK_PATH_EXPRESSION_VARIABLE, sortKeyName.get());
        } else {
            conditionalExpression = PK_EXISTS_AND_RVN_IS_THE_SAME_CONDITION;
        }

        if (updateExistingLockRecord) {
            item.remove(partitionKeyName);
            if (sortKeyName.isPresent()) {
                item.remove(sortKeyName.get());
            }
            final String updateExpression = getUpdateExpressionAndUpdateNameValueMaps(item, expressionAttributeNames, expressionAttributeValues);

            final UpdateItemRequest updateItemRequest = new UpdateItemRequest().withTableName(tableName).withKey(getItemKeys(existingLock.get()))
                    .withUpdateExpression(updateExpression).withExpressionAttributeNames(expressionAttributeNames)
                    .withExpressionAttributeValues(expressionAttributeValues).withConditionExpression(conditionalExpression);
            logger.trace("Acquiring an existing lock whose revisionVersionNumber did not change for " + partitionKeyName + " partitionKeyName=" + key + ", " + this.sortKeyName + "=" + sortKey);
            return updateItemAndStartSessionMonitor(options, key, sortKey, deleteLockOnRelease, sessionMonitor, newLockData, recordVersionNumber, updateItemRequest);
        } else {
            final PutItemRequest putItemRequest = new PutItemRequest().withItem(item).withTableName(tableName).withConditionExpression(conditionalExpression)
                    .withExpressionAttributeNames(expressionAttributeNames).withExpressionAttributeValues(expressionAttributeValues);

            logger.trace("Acquiring an existing lock whose revisionVersionNumber did not change for " + partitionKeyName + " partitionKeyName=" + key + ", " + this.sortKeyName + "=" + sortKey);
            return putLockItemAndStartSessionMonitor(options, key, sortKey, deleteLockOnRelease, sessionMonitor, newLockData, recordVersionNumber, putItemRequest);
        }

    }

    private LockItem upsertAndMonitorReleasedLock(AcquireLockOptions options, String key, Optional<String> sortKey, boolean
            deleteLockOnRelease, Optional<SessionMonitor> sessionMonitor, Optional<LockItem> existingLock, Optional<ByteBuffer>
            newLockData, Map<String, AttributeValue> item, String recordVersionNumber) {

        final String conditionalExpression;
        final boolean updateExistingLockRecord = options.getUpdateExistingLockRecord();
        final boolean consistentLockData = options.getAcquireReleasedLocksConsistently();

        final Map<String, String> expressionAttributeNames = new HashMap<>();
        final Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();

        if (consistentLockData) {
            expressionAttributeValues.put(RVN_VALUE_EXPRESSION_VARIABLE, new AttributeValue(existingLock.get().getRecordVersionNumber()));
            expressionAttributeNames.put(RVN_PATH_EXPRESSION_VARIABLE, RECORD_VERSION_NUMBER);
        }
        expressionAttributeNames.put(PK_PATH_EXPRESSION_VARIABLE, partitionKeyName);
        expressionAttributeNames.put(IS_RELEASED_PATH_EXPRESSION_VARIABLE, IS_RELEASED);

        if (this.sortKeyName.isPresent()) {
            //We do not check the owner here because the lock is expired and it is OK to overwrite the owner
            if (consistentLockData) {
                conditionalExpression = PK_EXISTS_AND_SK_EXISTS_AND_RVN_IS_THE_SAME_AND_IS_RELEASED_CONDITION;
            } else {
                conditionalExpression = PK_EXISTS_AND_SK_EXISTS_AND_IS_RELEASED_CONDITION;
            }
            expressionAttributeNames.put(SK_PATH_EXPRESSION_VARIABLE, sortKeyName.get());
        } else {
            if (consistentLockData) {
                conditionalExpression = PK_EXISTS_AND_RVN_IS_THE_SAME_AND_IS_RELEASED_CONDITION;
            } else {
                conditionalExpression = PK_EXISTS_AND_IS_RELEASED_CONDITION;
            }
        }
        expressionAttributeValues.put(IS_RELEASED_VALUE_EXPRESSION_VARIABLE, IS_RELEASED_ATTRIBUTE_VALUE);

        if (updateExistingLockRecord) {
            item.remove(partitionKeyName);
            if (sortKeyName.isPresent()) {
                item.remove(sortKeyName.get());
            }
            final String updateExpression = getUpdateExpressionAndUpdateNameValueMaps(item, expressionAttributeNames, expressionAttributeValues)
                    + REMOVE_IS_RELEASED_UPDATE_EXPRESSION;

            final UpdateItemRequest updateItemRequest = new UpdateItemRequest().withTableName(tableName).withKey(getItemKeys(existingLock.get()))
                    .withUpdateExpression(updateExpression).withExpressionAttributeNames(expressionAttributeNames)
                    .withExpressionAttributeValues(expressionAttributeValues).withConditionExpression(conditionalExpression);
            logger.trace("Acquiring an existing released whose revisionVersionNumber did not change for " + partitionKeyName + " " +
                                 "partitionKeyName=" + key + ", " + this.sortKeyName + "=" + sortKey);
            return updateItemAndStartSessionMonitor(options, key, sortKey, deleteLockOnRelease, sessionMonitor, newLockData, recordVersionNumber, updateItemRequest);
        } else {
            final PutItemRequest putItemRequest = new PutItemRequest().withItem(item).withTableName(tableName).withConditionExpression(conditionalExpression)
                    .withExpressionAttributeNames(expressionAttributeNames).withExpressionAttributeValues(expressionAttributeValues);

            logger.trace("Acquiring an existing released lock whose revisionVersionNumber did not change for " + partitionKeyName + " " +
                                 "partitionKeyName=" + key + ", " + this.sortKeyName + "=" + sortKey);
            return putLockItemAndStartSessionMonitor(options, key, sortKey, deleteLockOnRelease, sessionMonitor, newLockData, recordVersionNumber, putItemRequest);
        }

    }


    private LockItem updateItemAndStartSessionMonitor(AcquireLockOptions options, String key, Optional<String> sortKey, boolean deleteLockOnRelease,
                                                      Optional<SessionMonitor> sessionMonitor, Optional<ByteBuffer> newLockData, String recordVersionNumber, UpdateItemRequest updateItemRequest) {
        final long lastUpdatedTime = LockClientUtils.INSTANCE.millisecondTime();
        if (options.getRequestMetricCollector().isPresent()) {
            updateItemRequest.setRequestMetricCollector(options.getRequestMetricCollector().get());
        }
        this.dynamoDB.updateItem(updateItemRequest);
        final LockItem lockItem =
                new LockItem(this, key, sortKey, newLockData, deleteLockOnRelease, this.ownerName, this.leaseDurationInMilliseconds, lastUpdatedTime,
                        recordVersionNumber, !IS_RELEASED_INDICATOR, sessionMonitor, options.getAdditionalAttributes());
        this.locks.put(lockItem.getUniqueIdentifier(), lockItem);
        this.tryAddSessionMonitor(lockItem.getUniqueIdentifier(), lockItem);
        return lockItem;
    }

    /**
     * This method puts a new lock item in the lock table and returns an optionally monitored LockItem object
     * @param options a wrapper of RequestMetricCollector and an "additional attributes" map
     * @param key the partition key of the lock to write
     * @param sortKey the optional sort key of the lock to write
     * @param deleteLockOnRelease whether or not to delete the lock when releasing it
     * @param sessionMonitor the optional session monitor to start for this lock
     * @param newLockData the new lock data
     * @param item the lock item to write to the lock table
     * @param recordVersionNumber the rvn to condition the PutItem call on.
     * @return a new monitored LockItem
     */
    private LockItem upsertAndMonitorNewLock(AcquireLockOptions options, String key, Optional<String> sortKey,
        boolean deleteLockOnRelease, Optional<SessionMonitor> sessionMonitor,
        Optional<ByteBuffer> newLockData, Map<String, AttributeValue> item, String recordVersionNumber) {

        final Map<String, String> expressionAttributeNames = new HashMap<>();
        expressionAttributeNames.put(PK_PATH_EXPRESSION_VARIABLE, this.partitionKeyName);

        final boolean updateExistingLockRecord = options.getUpdateExistingLockRecord();

        final String conditionalExpression;
        if (this.sortKeyName.isPresent()) {
            conditionalExpression = ACQUIRE_LOCK_THAT_DOESNT_EXIST_PK_SK_CONDITION;
            expressionAttributeNames.put(SK_PATH_EXPRESSION_VARIABLE, sortKeyName.get());
        } else {
            conditionalExpression = ACQUIRE_LOCK_THAT_DOESNT_EXIST_PK_CONDITION;
        }

        if (updateExistingLockRecord) {
            // Remove keys from item to create updateExpression
            item.remove(partitionKeyName);
            if (sortKeyName.isPresent()) {
                item.remove(sortKeyName.get());
            }
            final Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
            final String updateExpression = getUpdateExpressionAndUpdateNameValueMaps(item, expressionAttributeNames, expressionAttributeValues);
            final UpdateItemRequest updateItemRequest = new UpdateItemRequest().withTableName(tableName).withKey(getKeys(key, sortKey))
                    .withUpdateExpression(updateExpression).withExpressionAttributeNames(expressionAttributeNames)
                    .withExpressionAttributeValues(expressionAttributeValues).withConditionExpression(conditionalExpression);
            logger.trace("Acquiring a new lock on " + partitionKeyName + "=" + key + ", " + this.sortKeyName + "=" + sortKey);
            return updateItemAndStartSessionMonitor(options, key, sortKey, deleteLockOnRelease, sessionMonitor, newLockData, recordVersionNumber, updateItemRequest);
        } else {
            final PutItemRequest putItemRequest = new PutItemRequest().withItem(item).withTableName(tableName)
                    .withConditionExpression(conditionalExpression)
                    .withExpressionAttributeNames(expressionAttributeNames);
        /* No one has the lock, go ahead and acquire it.
         * The person storing the lock into DynamoDB should err on the side of thinking the lock will expire
         * sooner than it actually will, so they start counting towards its expiration before the Put succeeds
         */
            logger.trace("Acquiring a new lock on " + partitionKeyName + "=" + key + ", " + this.sortKeyName + "=" + sortKey);
            return putLockItemAndStartSessionMonitor(options, key, sortKey, deleteLockOnRelease, sessionMonitor, newLockData, recordVersionNumber, putItemRequest);
        }
    }

    private LockItem putLockItemAndStartSessionMonitor(AcquireLockOptions options, String key, Optional<String> sortKey, boolean deleteLockOnRelease,
        Optional<SessionMonitor> sessionMonitor, Optional<ByteBuffer> newLockData, String recordVersionNumber, PutItemRequest putItemRequest) {
        final long lastUpdatedTime = LockClientUtils.INSTANCE.millisecondTime();
        if (options.getRequestMetricCollector().isPresent()) {
            putItemRequest.setRequestMetricCollector(options.getRequestMetricCollector().get());
        }
        this.dynamoDB.putItem(putItemRequest);

        final LockItem lockItem =
            new LockItem(this, key, sortKey, newLockData, deleteLockOnRelease, this.ownerName, this.leaseDurationInMilliseconds, lastUpdatedTime,
                recordVersionNumber, false, sessionMonitor, options.getAdditionalAttributes());
        this.locks.put(lockItem.getUniqueIdentifier(), lockItem);
        this.tryAddSessionMonitor(lockItem.getUniqueIdentifier(), lockItem);
        return lockItem;
    }

    /**
     * Builds an updateExpression for all fields in item map and updates the correspoding expression attribute name and
     * value maps.
     * @param item Map of Name and AttributeValue to update or create
     * @param expressionAttributeNames
     * @param expressionAttributeValues
     * @return
     */
    private String getUpdateExpressionAndUpdateNameValueMaps(Map<String, AttributeValue> item,
        Map<String, String> expressionAttributeNames, Map<String, AttributeValue> expressionAttributeValues) {
        final String additionalUpdateExpression = "SET ";
        StringBuilder updateExpressionBuilder = new StringBuilder(additionalUpdateExpression);
        int i = 0;
        String keyExpression;
        String valueExpression;
        Iterator<Entry<String, AttributeValue>> iterator = item.entrySet().iterator();
        String expressionSeparator = ",";
        while (iterator.hasNext()) {
            Entry<String, AttributeValue> entry = iterator.next();
            keyExpression = "#k" + i;
            valueExpression = ":v" + i;

            expressionAttributeNames.put(keyExpression, entry.getKey());
            expressionAttributeValues.put(valueExpression, entry.getValue());
            if (!iterator.hasNext()) {
                expressionSeparator = "";
            }
            updateExpressionBuilder.append("#k").append(i).append("=").append(":v").append(i).append(expressionSeparator);
            i++;
        }
        return updateExpressionBuilder.toString();
    }

    /**
     * Attempts to acquire lock. If successful, returns the lock. Otherwise,
     * returns Optional.empty(). For more details on behavior, please see
     * {@code acquireLock}.
     *
     * @param options The options to use when acquiring the lock.
     * @return the lock if successful.
     * @throws InterruptedException in case this.acquireLock was interrupted.
     */
    public Optional<LockItem> tryAcquireLock(final AcquireLockOptions options) throws InterruptedException {
        try {
            return Optional.of(this.acquireLock(options));
        } catch (final LockNotGrantedException x) {
            return Optional.empty();
        }
    }

    /**
     * Releases the given lock if the current user still has it, returning true if the lock was successfully released, and false
     * if someone else already stole the lock. Deletes the lock item if it is released and deleteLockItemOnClose is set.
     *
     * @param lockItem The lock item to release
     * @return true if the lock is released, false otherwise
     */
    public boolean releaseLock(final LockItem lockItem) {
        return this.releaseLock(ReleaseLockOptions.builder(lockItem).withDeleteLock(lockItem.getDeleteLockItemOnClose()).build());
    }

    public boolean releaseLock(final ReleaseLockOptions options) {
        Objects.requireNonNull(options, "ReleaseLockOptions cannot be null");

        final LockItem lockItem = options.getLockItem();
        final boolean deleteLock = options.isDeleteLock();
        final boolean bestEffort = options.isBestEffort();
        final Optional<ByteBuffer> data = options.getData();
        Objects.requireNonNull(lockItem, "Cannot release null lockItem");
        if (!lockItem.getOwnerName().equals(this.ownerName)) {
            return false;
        }

        synchronized (lockItem) {
            try {
                // Always remove the heartbeat for the lock. The
                // caller's intention is to release the lock. Stopping the
                // heartbeat alone will do that regardless of whether the Dynamo
                // write succeeds or fails.
                this.locks.remove(lockItem.getUniqueIdentifier());

                //set up expression stuff for DeleteItem or UpdateItem
                //basically any changes require:
                //1. I own the lock
                //2. I know the current version number
                //3. The lock already exists (UpdateItem API can cause a new item to be created if you do not condition the primary keys with attribute_exists)
                final String conditionalExpression;
                final Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
                expressionAttributeValues.put(RVN_VALUE_EXPRESSION_VARIABLE, new AttributeValue(lockItem.getRecordVersionNumber()));
                expressionAttributeValues.put(OWNER_NAME_VALUE_EXPRESSION_VARIABLE, new AttributeValue(lockItem.getOwnerName()));
                final Map<String, String> expressionAttributeNames = new HashMap<>();
                expressionAttributeNames.put(PK_PATH_EXPRESSION_VARIABLE, partitionKeyName);
                expressionAttributeNames.put(OWNER_NAME_PATH_EXPRESSION_VARIABLE, OWNER_NAME);
                expressionAttributeNames.put(RVN_PATH_EXPRESSION_VARIABLE, RECORD_VERSION_NUMBER);
                if (this.sortKeyName.isPresent()) {
                    conditionalExpression = PK_EXISTS_AND_SK_EXISTS_AND_OWNER_NAME_SAME_AND_RVN_SAME_CONDITION;
                    expressionAttributeNames.put(SK_PATH_EXPRESSION_VARIABLE, sortKeyName.get());
                } else {
                    conditionalExpression = PK_EXISTS_AND_OWNER_NAME_SAME_AND_RVN_SAME_CONDITION;
                }

                final Map<String, AttributeValue> key = getItemKeys(lockItem);
                if (deleteLock) {
                    final DeleteItemRequest deleteItemRequest = new DeleteItemRequest(this.tableName, key);
                    if (options.getRequestMetricCollector().isPresent()) {
                        deleteItemRequest.setRequestMetricCollector(options.getRequestMetricCollector().get());
                    }

                    this.dynamoDB.deleteItem(deleteItemRequest.withConditionExpression(conditionalExpression).withExpressionAttributeNames(expressionAttributeNames)
                        .withExpressionAttributeValues(expressionAttributeValues));
                } else {
                    final String updateExpression;
                    expressionAttributeNames.put(IS_RELEASED_PATH_EXPRESSION_VARIABLE, IS_RELEASED);
                    expressionAttributeValues.put(IS_RELEASED_VALUE_EXPRESSION_VARIABLE, IS_RELEASED_ATTRIBUTE_VALUE);
                    if (data.isPresent()) {
                        updateExpression = UPDATE_IS_RELEASED_AND_DATA;
                        expressionAttributeNames.put(DATA_PATH_EXPRESSION_VARIABLE, DATA);
                        expressionAttributeValues.put(DATA_VALUE_EXPRESSION_VARIABLE, new AttributeValue().withB(data.get()));
                    } else {
                        updateExpression = UPDATE_IS_RELEASED;
                    }
                    final UpdateItemRequest updateItemRequest =
                        new UpdateItemRequest().withTableName(this.tableName).withKey(key).withUpdateExpression(updateExpression).withConditionExpression(conditionalExpression)
                            .withExpressionAttributeNames(expressionAttributeNames).withExpressionAttributeValues(expressionAttributeValues);

                    if (options.getRequestMetricCollector().isPresent()) {
                        updateItemRequest.setRequestMetricCollector(options.getRequestMetricCollector().get());
                    }

                    this.dynamoDB.updateItem(updateItemRequest);
                }
            } catch (final ConditionalCheckFailedException conditionalCheckFailedException) {
                logger.debug("Someone else acquired the lock before you asked to release it", conditionalCheckFailedException);
                return false;
            } catch (final AmazonClientException amazonClientException) {
                if (bestEffort) {
                    logger.warn("Ignore AmazonClientException and continue to clean up", amazonClientException);
                } else {
                    throw amazonClientException;
                }
            }

            // Only remove the session monitor if no exception thrown above.
            // While moving the heartbeat removal before the DynamoDB call
            // should not cause existing clients problems, there
            // may be existing clients that depend on the monitor firing if they
            // get exceptions from this method.
            this.removeKillSessionMonitor(lockItem.getUniqueIdentifier());
        }
        return true;
    }

    private Map<String, AttributeValue> getItemKeys(LockItem lockItem) {
        return getKeys(lockItem.getPartitionKey(), lockItem.getSortKey());
    }

    private Map<String, AttributeValue> getKeys(String partitionKey, Optional<String> sortKey) {
        final Map<String, AttributeValue> key = new HashMap<>();
        key.put(this.partitionKeyName, new AttributeValue().withS(partitionKey));
        if (sortKey.isPresent()) {
            key.put(this.sortKeyName.get(), new AttributeValue().withS(sortKey.get()));
        }
        return key;
    }

    /**
     * Releases all the locks currently held by the owner specified when creating this lock client
     */
    private void releaseAllLocks() {
        final Map<String, LockItem> locks = new HashMap<>(this.locks);
        synchronized (locks) {
            for (final Entry<String, LockItem> lockEntry : locks.entrySet()) {
                this.releaseLock(lockEntry.getValue()); // TODO catch exceptions and report failure separately
            }
        }
    }

    /**
     * Finds out who owns the given lock, but does not acquire the lock. It returns the metadata currently associated with the
     * given lock. If the client currently has the lock, it will return the lock, and operations such as releaseLock will work.
     * However, if the client does not have the lock, then operations like releaseLock will not work (after calling getLock, the
     * caller should check lockItem.isExpired() to figure out if it currently has the lock.)
     *
     * @param key     The partition key representing the lock.
     * @param sortKey The sort key if present.
     * @return A LockItem that represents the lock, if the lock exists.
     */
    public Optional<LockItem> getLock(final String key, final Optional<String> sortKey) {
        Objects.requireNonNull(sortKey, "Sort Key must not be null (can be Optional.empty())");
        final LockItem localLock = this.locks.get(key + sortKey.orElse(""));
        if (localLock != null) {
            return Optional.of(localLock);
        }
        final Optional<LockItem> lockItem =
            this.getLockFromDynamoDB(new GetLockOptions.GetLockOptionsBuilder(key).withSortKey(sortKey.orElse(null)).withDeleteLockOnRelease(false).build());

        if (lockItem.isPresent()) {
            if (lockItem.get().isReleased()) {
                // Return empty if a lock was released but still left in the table
                return Optional.empty();
            } else {
                /*
                 * Clear out the record version number so that the caller cannot accidentally perform updates on this lock (since
                 * the caller has not acquired the lock)
                 */
                lockItem.get().updateRecordVersionNumber("", 0, lockItem.get().getLeaseDuration());
            }
        }

        return lockItem;
    }

    /**
     * Retrieves the lock item from DynamoDB. Note that this will return a
     * LockItem even if it was released -- do NOT use this method if your goal
     * is to acquire a lock for doing work.
     *
     * @param options The options such as the key, etc.
     * @return The LockItem, or absent if it is not present. Note that the item
     * can exist in the table even if it is released, as noted by
     * isReleased().
     */
    public Optional<LockItem> getLockFromDynamoDB(final GetLockOptions options) {
        Objects.requireNonNull(options, "AcquireLockOptions cannot be null");
        Objects.requireNonNull(options.getPartitionKey(), "Cannot lookup null key");
        final GetItemResult result = this.readFromDynamoDB(options.getPartitionKey(), options.getSortKey(), options.getRequestMetricCollector());
        final Map<String, AttributeValue> item = result.getItem();

        if (item == null) {
            return Optional.empty();
        }

        return Optional.of(this.createLockItem(options, item));
    }

    private LockItem createLockItem(final GetLockOptions options, final Map<String, AttributeValue> item) {
        final Optional<ByteBuffer> data = Optional.ofNullable(item.get(DATA)).map(dataAttributionValue -> {
            item.remove(DATA);
            return dataAttributionValue.getB();
        });

        final AttributeValue ownerName = item.remove(OWNER_NAME);
        final AttributeValue leaseDuration = item.remove(LEASE_DURATION);
        final AttributeValue recordVersionNumber = item.remove(RECORD_VERSION_NUMBER);

        final boolean isReleased = item.containsKey(IS_RELEASED);
        item.remove(IS_RELEASED);
        item.remove(this.partitionKeyName);

        /*
         * The person retrieving the lock in DynamoDB should err on the side of
         * not expiring the lock, so they don't start counting until after the
         * call to DynamoDB succeeds
         */
        final long lookupTime = LockClientUtils.INSTANCE.millisecondTime();
        final LockItem lockItem =
            new LockItem(this,
                options.getPartitionKey(),
                options.getSortKey(), data,
                options.isDeleteLockOnRelease(),
                ownerName.getS(),
                Long.parseLong(leaseDuration.getS()), lookupTime,
                recordVersionNumber.getS(), isReleased, Optional.empty(), item);
        return lockItem;
    }

    /**
     * <p>
     * Retrieves all the lock items from DynamoDB.
     * </p>
     * <p>
     * Not that this will may return a lock item even if it was released.
     * </p>
     *
     * @param deleteOnRelease Whether or not the {@link LockItem} should delete the item
     *                        when {@link LockItem#close()} is called on it.
     * @return A non parallel {@link Stream} of all the {@link LockItem}s in
     * DynamoDB. Note that the item can exist in the table even if it is
     * released, as noted by @{link LockItem#isReleased()}.
     */
    public Stream<LockItem> getAllLocksFromDynamoDB(final boolean deleteOnRelease) {
        final ScanRequest scanRequest = new ScanRequest().withTableName(this.tableName);
        final LockItemPaginatedScanIterator iterator = new LockItemPaginatedScanIterator(this.dynamoDB, scanRequest, item -> {
            final String key = item.get(this.partitionKeyName).getS();
            GetLockOptionsBuilder options = GetLockOptions.builder(key).withDeleteLockOnRelease(deleteOnRelease);

            options = this.sortKeyName.map(item::get).map(AttributeValue::getS).map(options::withSortKey).orElse(options);

            final LockItem lockItem = this.createLockItem(options.build(), item);
            return lockItem;
        });

        final Iterable<LockItem> iterable = () -> iterator;
        return StreamSupport.stream(iterable.spliterator(), false /*isParallelStream*/);
    }

    /**
     * <p>
     * Sends a heartbeat to indicate that the given lock is still being worked on. If using
     * {@code createHeartbeatBackgroundThread}=true when setting up this object, then this method is unnecessary, because the
     * background thread will be periodically calling it and sending heartbeats. However, if
     * {@code createHeartbeatBackgroundThread}=false, then this method must be called to instruct DynamoDB that the lock should
     * not be expired.
     * </p>
     * <p>
     * The lease duration of the lock will be set to the default specified in the constructor of this class.
     * </p>
     *
     * @param lockItem the lock item row to send a heartbeat and extend lock expiry.
     */
    public void sendHeartbeat(final LockItem lockItem) {
        this.sendHeartbeat(SendHeartbeatOptions.builder(lockItem).build());
    }

    /**
     * <p>
     * Sends a heartbeat to indicate that the given lock is still being worked on. If using
     * {@code createHeartbeatBackgroundThread}=true when setting up this object, then this method is unnecessary, because the
     * background thread will be periodically calling it and sending heartbeats. However, if
     * {@code createHeartbeatBackgroundThread}=false, then this method must be called to instruct DynamoDB that the lock should
     * not be expired.
     * </p>
     * <p>
     * This method will also set the lease duration of the lock to the given value.
     * </p>
     * <p>
     * This will also either update or delete the data from the lock, as specified in the options
     * </p>
     *
     * @param options a set of optional arguments for how to send the heartbeat
     */
    public void sendHeartbeat(final SendHeartbeatOptions options) {
        Objects.requireNonNull(options, "options is required");
        Objects.requireNonNull(options.getLockItem(), "Cannot send heartbeat for null lock");
        final boolean deleteData = options.getDeleteData() != null && options.getDeleteData();
        if (deleteData && options.getData().isPresent()) {
            throw new IllegalArgumentException("data must not be present if deleteData is true");
        }

        long leaseDurationToEnsureInMilliseconds = this.leaseDurationInMilliseconds;
        if (options.getLeaseDurationToEnsure() != null) {
            Objects.requireNonNull(options.getTimeUnit(), "TimeUnit must not be null if leaseDurationToEnsure is not null");
            leaseDurationToEnsureInMilliseconds = options.getTimeUnit().toMillis(options.getLeaseDurationToEnsure());
        }

        final LockItem lockItem = options.getLockItem();
        if (lockItem.isExpired() || !lockItem.getOwnerName().equals(this.ownerName) || lockItem.isReleased()) {
            this.locks.remove(lockItem.getUniqueIdentifier());
            throw new LockNotGrantedException("Cannot send heartbeat because lock is not granted");
        }

        synchronized (lockItem) {
            //Set up condition for UpdateItem. Basically any changes require:
            //1. I own the lock
            //2. I know the current version number
            //3. The lock already exists (UpdateItem API can cause a new item to be created if you do not condition the primary keys with attribute_exists)
            final String conditionalExpression;
            final Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
            expressionAttributeValues.put(RVN_VALUE_EXPRESSION_VARIABLE, new AttributeValue(lockItem.getRecordVersionNumber()));
            expressionAttributeValues.put(OWNER_NAME_VALUE_EXPRESSION_VARIABLE, new AttributeValue(lockItem.getOwnerName()));
            final Map<String, String> expressionAttributeNames = new HashMap<>();
            expressionAttributeNames.put(PK_PATH_EXPRESSION_VARIABLE, partitionKeyName);
            expressionAttributeNames.put(LEASE_DURATION_PATH_VALUE_EXPRESSION_VARIABLE, LEASE_DURATION);
            expressionAttributeNames.put(RVN_PATH_EXPRESSION_VARIABLE, RECORD_VERSION_NUMBER);
            expressionAttributeNames.put(OWNER_NAME_PATH_EXPRESSION_VARIABLE, OWNER_NAME);
            if (this.sortKeyName.isPresent()) {
                conditionalExpression = PK_EXISTS_AND_SK_EXISTS_AND_OWNER_NAME_SAME_AND_RVN_SAME_CONDITION;
                expressionAttributeNames.put(SK_PATH_EXPRESSION_VARIABLE, sortKeyName.get());
            } else {
                conditionalExpression = PK_EXISTS_AND_OWNER_NAME_SAME_AND_RVN_SAME_CONDITION;
            }

            final String recordVersionNumber = this.generateRecordVersionNumber();

            //Set up update expression for UpdateItem.
            final String updateExpression;
            expressionAttributeValues.put(NEW_RVN_VALUE_EXPRESSION_VARIABLE, new AttributeValue(recordVersionNumber));
            expressionAttributeValues.put(LEASE_DURATION_VALUE_EXPRESSION_VARIABLE, new AttributeValue(String.valueOf(leaseDurationToEnsureInMilliseconds)));
            if (deleteData) {
                expressionAttributeNames.put(DATA_PATH_EXPRESSION_VARIABLE, DATA);
                updateExpression = UPDATE_LEASE_DURATION_AND_RVN_AND_REMOVE_DATA;
            } else if (options.getData().isPresent()) {
                expressionAttributeNames.put(DATA_PATH_EXPRESSION_VARIABLE, DATA);
                expressionAttributeValues.put(DATA_VALUE_EXPRESSION_VARIABLE, new AttributeValue().withB(options.getData().get()));
                updateExpression = UPDATE_LEASE_DURATION_AND_RVN_AND_DATA;
            } else {
                updateExpression = UPDATE_LEASE_DURATION_AND_RVN;
            }

            final UpdateItemRequest updateItemRequest =
                new UpdateItemRequest().withTableName(tableName).withKey(getItemKeys(lockItem)).withConditionExpression(conditionalExpression).withUpdateExpression(updateExpression)
                    .withExpressionAttributeNames(expressionAttributeNames).withExpressionAttributeValues(expressionAttributeValues);

            if (options.getRequestMetricCollector().isPresent()) {
                updateItemRequest.setRequestMetricCollector(options.getRequestMetricCollector().get());
            }

            try {
                final long lastUpdateOfLock = LockClientUtils.INSTANCE.millisecondTime();
                this.dynamoDB.updateItem(updateItemRequest);
                lockItem.updateRecordVersionNumber(recordVersionNumber, lastUpdateOfLock, leaseDurationToEnsureInMilliseconds);
            } catch (final ConditionalCheckFailedException conditionalCheckFailedException) {
                logger.debug("Someone else acquired the lock, so we will stop heartbeating it", conditionalCheckFailedException);
                this.locks.remove(lockItem.getUniqueIdentifier());
                throw new LockNotGrantedException("Someone else acquired the lock, so we will stop heartbeating it", conditionalCheckFailedException);
            } catch (AmazonServiceException amazonServiceException) {
                if (holdLockOnServiceUnavailable && amazonServiceException.getStatusCode() == HttpStatus.SC_SERVICE_UNAVAILABLE) {
                    // When DynamoDB service is unavailable, other threads may get the same exception and no thread may have the lock.
                    // For systems which should always hold a lock on an item and it is okay for multiple threads to hold the lock,
                    // the lookUpTime of local state can be updated to make it believe that it still has the lock.
                    logger.info("DynamoDB Service Unavailable. Holding the lock.");
                    lockItem.updateLookUpTime(LockClientUtils.INSTANCE.millisecondTime());
                } else {
                    throw amazonServiceException;
                }
            }
        }
    }

    /**
     * Loops forever, sending hearbeats for all the locks this thread needs to keep track of.
     */
    @Override
    public void run() {
        while (true) {
            try {
                if (this.shuttingDown) {
                    throw new InterruptedException(); // sometimes libraries wrap interrupted and other exceptions
                }
                final long timeWorkBegins = LockClientUtils.INSTANCE.millisecondTime();
                final Map<String, LockItem> workingCopyOfLocks = new HashMap<>(this.locks);
                for (final Entry<String, LockItem> lockEntry : workingCopyOfLocks.entrySet()) {
                    try {
                        this.sendHeartbeat(lockEntry.getValue());
                    } catch (final LockNotGrantedException x) {
                        logger.debug("Heartbeat failed for " + lockEntry, x);
                    } catch (final RuntimeException x) {
                        logger.warn("Exception sending heartbeat for " + lockEntry, x);
                    }
                }
                final long timeElapsed = LockClientUtils.INSTANCE.millisecondTime() - timeWorkBegins;

                if (this.shuttingDown) {
                    throw new InterruptedException(); // sometimes libraries wrap interrupted and other exceptions
                }

                /* If we want to hearbeat every 9 seconds, and it took 3 seconds to send the heartbeats, we only sleep 6 seconds */
                Thread.sleep(Math.max(this.heartbeatPeriodInMilliseconds - timeElapsed, 0));
            } catch (final InterruptedException e) {
                logger.info("Heartbeat thread recieved interrupt, exiting run() (possibly exiting thread)", e);
                return;
            } catch (final RuntimeException x) {
                logger.warn("Exception sending heartbeat", x);
            }
        }
    }

    /**
     * Releases all of the locks by calling releaseAllLocks()
     */
    @Override
    public void close() throws IOException {
        // release the locks before interrupting the heartbeat thread to avoid partially updated/stale locks
        this.releaseAllLocks();
        if (this.backgroundThread.isPresent()) {
            this.shuttingDown = true;
            this.backgroundThread.get().interrupt();
            try {
                this.backgroundThread.get().join();
            } catch (final InterruptedException e) {
                logger.warn("Caught InterruptedException waiting for background thread to exit, interrupting current thread");
                Thread.currentThread().interrupt();
            }
        }
    }

    /* Helper method to read a key from DynamoDB */
    private GetItemResult readFromDynamoDB(final String key, final Optional<String> sortKey, final Optional<RequestMetricCollector> requestMetricCollector) {
        final Map<String, AttributeValue> dynamoDBKey = new HashMap<>();
        dynamoDBKey.put(this.partitionKeyName, new AttributeValue().withS(key));
        if (this.sortKeyName.isPresent()) {
            dynamoDBKey.put(this.sortKeyName.get(), new AttributeValue().withS(sortKey.get()));
        }
        final GetItemRequest getItemRequest = new GetItemRequest(this.tableName, dynamoDBKey);
        getItemRequest.setConsistentRead(true);

        if (requestMetricCollector.isPresent()) {
            getItemRequest.setRequestMetricCollector(requestMetricCollector.get());
        }

        return this.dynamoDB.getItem(getItemRequest);
    }

    /* Helper method that starts a background heartbeating thread */
    private Thread startBackgroundThread() {
        final Thread t = namedThreadCreator
            .apply("dynamodb-lock-client-" + lockClientId.addAndGet(1))
            .newThread(this);
        t.setDaemon(true);
        t.start();
        return t;
    }

    /*
     * Generates a UUID for the record version number. Note that using something like an increasing sequence ID for the record
     * version number doesn't work, because it introduces race conditions into the logic, which could allow different threads to
     * steal each other's locks.
     */
    private String generateRecordVersionNumber() {
        return UUID.randomUUID().toString();
    }

    private void tryAddSessionMonitor(final String lockName, final LockItem lock) {
        if (lock.hasSessionMonitor() && lock.hasCallback()) {
            final Thread monitorThread = lockSessionMonitorChecker(lockName, lock);
            monitorThread.setDaemon(true);
            monitorThread.start();
            this.sessionMonitors.put(lockName, monitorThread);
        }
    }

    private void removeKillSessionMonitor(final String monitorName) {
        if (this.sessionMonitors.containsKey(monitorName)) {
            final Thread monitor = this.sessionMonitors.remove(monitorName);
            monitor.interrupt();
            try {
                monitor.join();
            } catch (final InterruptedException e) {
                logger.warn("Caught InterruptedException waiting for session monitor thread to exit, ignoring");
            }
        }
    }

    /*
     * Validates the arguments to ensure that they are safe to register a
     * SessionMonitor on the lock to be acquired.
     *
     * @param safeTimeWithoutHeartbeatMillis the amount of time (in milliseconds) a lock can go without
     * heartbeating before it is declared to be in the "danger zone"
     *
     * @param heartbeatPeriodMillis the heartbeat period (in milliseconds)
     *
     * @param leaseDurationMillis the lease duration (in milliseconds)
     *
     * @throws IllegalArgumentException when the safeTimeWithoutHeartbeat is
     * less than the heartbeat frequency or greater than the lease duration
     */
    private static void sessionMonitorArgsValidate(final long safeTimeWithoutHeartbeatMillis, final long heartbeatPeriodMillis, final long leaseDurationMillis)
        throws IllegalArgumentException {
        if (safeTimeWithoutHeartbeatMillis <= heartbeatPeriodMillis) {
            throw new IllegalArgumentException("safeTimeWithoutHeartbeat must be greater than heartbeat frequency");
        } else if (safeTimeWithoutHeartbeatMillis >= leaseDurationMillis) {
            throw new IllegalArgumentException("safeTimeWithoutHeartbeat must be less than the lock's lease duration");
        }
    }

    private Thread lockSessionMonitorChecker(final String monitorName, final LockItem lock) {
        return namedThreadCreator.apply(monitorName + "-sessionMonitor").newThread(() -> {
            while (true) {
                try {
                    final long millisUntilDangerZone = lock.millisecondsUntilDangerZoneEntered();
                    if (millisUntilDangerZone > 0) {
                        Thread.sleep(millisUntilDangerZone);
                    } else {
                        lock.runSessionMonitor();
                        sessionMonitors.remove(monitorName);
                        return;
                    }
                } catch (final InterruptedException e) {
                    return;
                }
            }
        });
    }
}
