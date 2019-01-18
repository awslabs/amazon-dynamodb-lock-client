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

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * An options class for setting up a lock client with various overrides to the defaults.
 * In order to use this, it must be constructed using the builder.
 *
 * @author <a href="mailto:slutsker@amazon.com">Sasha Slutsker</a>
 */
public class AmazonDynamoDBLockClientOptions {
    protected static final String DEFAULT_PARTITION_KEY_NAME = "key";
    protected static final Long DEFAULT_LEASE_DURATION = 20L;
    protected static final Long DEFAULT_HEARTBEAT_PERIOD = 5L;
    protected static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.SECONDS;
    protected static final Boolean DEFAULT_CREATE_HEARTBEAT_BACKGROUND_THREAD = true;
    protected static final Boolean DEFAULT_HOLD_LOCK_ON_SERVICE_UNAVAILABLE = false;

    private final AmazonDynamoDB dynamoDBClient;
    private final String tableName;
    private final String partitionKeyName;
    private final Optional<String> sortKeyName;
    private final String ownerName;
    private final Long leaseDuration;
    private final Long heartbeatPeriod;
    private final TimeUnit timeUnit;
    private final Boolean createHeartbeatBackgroundThread;
    private final Function<String, ThreadFactory> namedThreadCreator;
    private final Boolean holdLockOnServiceUnavailable;


    /**
     * A builder for setting up an AmazonDynamoDBLockClientOptions object. By default, it is setup to have a partition key name of
     * "key," a lease duration of 20 seconds, and a default heartbeat period of 5 seconds. These defaults can be overriden.
     */
    public static class AmazonDynamoDBLockClientOptionsBuilder {
        private AmazonDynamoDB dynamoDBClient;
        private String tableName;
        private String partitionKeyName;
        private Optional<String> sortKeyName;
        private String ownerName;
        private Long leaseDuration;
        private Long heartbeatPeriod;
        private TimeUnit timeUnit;
        private Boolean createHeartbeatBackgroundThread;
        private Boolean holdLockOnServiceUnavailable;
        private Function<String, ThreadFactory> namedThreadCreator;

        AmazonDynamoDBLockClientOptionsBuilder(final AmazonDynamoDB dynamoDBClient, final String tableName) {
            this(dynamoDBClient, tableName,
                    /* By default, tries to set ownerName to the localhost */
                generateOwnerNameFromLocalhost(),
                namedThreadCreator());
        }

        private static final String generateOwnerNameFromLocalhost() {
            try {
                return Inet4Address.getLocalHost().getHostName() + UUID.randomUUID().toString();
            } catch (final UnknownHostException e) {
                return UUID.randomUUID().toString();
            }
        }

        private static Function<String, ThreadFactory> namedThreadCreator() {
            return (String threadName) -> (Runnable runnable) -> new Thread(runnable, threadName);
        }

        AmazonDynamoDBLockClientOptionsBuilder(final AmazonDynamoDB dynamoDBClient, final String tableName, final String ownerName,
            final Function<String, ThreadFactory> namedThreadCreator) {
            this.dynamoDBClient = dynamoDBClient;
            this.tableName = tableName;
            this.partitionKeyName = DEFAULT_PARTITION_KEY_NAME;
            this.leaseDuration = DEFAULT_LEASE_DURATION;
            this.heartbeatPeriod = DEFAULT_HEARTBEAT_PERIOD;
            this.timeUnit = DEFAULT_TIME_UNIT;
            this.createHeartbeatBackgroundThread = DEFAULT_CREATE_HEARTBEAT_BACKGROUND_THREAD;
            this.sortKeyName = Optional.empty();
            this.ownerName = ownerName == null ? generateOwnerNameFromLocalhost() : ownerName;
            this.namedThreadCreator = namedThreadCreator == null ? namedThreadCreator() : namedThreadCreator;
            this.holdLockOnServiceUnavailable = DEFAULT_HOLD_LOCK_ON_SERVICE_UNAVAILABLE;
        }

        AmazonDynamoDBLockClientOptionsBuilder(final AmazonDynamoDB dynamoDBClient, final String tableName, final String ownerName) {
            this(dynamoDBClient, tableName, ownerName, namedThreadCreator());
        }

        /**
         * @param partitionKeyName The partition key name. If not specified, the default partition key name of "key" is used.
         * @return a reference to this builder for fluent method chaining
         */
        public AmazonDynamoDBLockClientOptionsBuilder withPartitionKeyName(final String partitionKeyName) {
            this.partitionKeyName = partitionKeyName;
            return this;
        }

        /**
         * @param sortKeyName The sort key name. If not specified, we assume that the table does not have a sort key defined.
         * @return a reference to this builder for fluent method chaining
         */
        public AmazonDynamoDBLockClientOptionsBuilder withSortKeyName(final String sortKeyName) {
            this.sortKeyName = Optional.of(sortKeyName);
            return this;
        }

        /**
         * @param ownerName The person that is acquiring the lock (for example, box.amazon.com)
         * @return a reference to this builder for fluent method chaining
         */
        public AmazonDynamoDBLockClientOptionsBuilder withOwnerName(final String ownerName) {
            this.ownerName = ownerName;
            return this;
        }

        /**
         * @param leaseDuration The length of time that the lease for the lock will be
         *                      granted for. If this is set to, for example, 30 seconds,
         *                      then the lock will expire if the heartbeat is not sent for
         *                      at least 30 seconds (which would happen if the box or the
         *                      heartbeat thread dies, for example.)
         * @return a reference to this builder for fluent method chaining
         */
        public AmazonDynamoDBLockClientOptionsBuilder withLeaseDuration(final Long leaseDuration) {
            this.leaseDuration = leaseDuration;
            return this;
        }

        /**
         * @param heartbeatPeriod How often to update DynamoDB to note that the instance is
         *                        still running (recommendation is to make this at least 3
         *                        times smaller than the leaseDuration -- for example
         *                        heartBeatPeriod=1 second, leaseDuration=10 seconds could
         *                        be a reasonable configuration, make sure to include a
         *                        buffer for network latency.)
         * @return a reference to this builder for fluent method chaining
         */
        public AmazonDynamoDBLockClientOptionsBuilder withHeartbeatPeriod(final Long heartbeatPeriod) {
            this.heartbeatPeriod = heartbeatPeriod;
            return this;
        }

        /**
         * @param timeUnit What time unit to use for all times in this object, including
         *                 heartbeatPeriod and leaseDuration.
         * @return a reference to this builder for fluent method chaining
         */
        public AmazonDynamoDBLockClientOptionsBuilder withTimeUnit(final TimeUnit timeUnit) {
            this.timeUnit = timeUnit;
            return this;
        }

        /**
         * @param createHeartbeatBackgroundThread Whether or not to create a thread to automatically
         *                                        heartbeat (if false, you must call sendHeartbeat manually)
         * @return a reference to this builder for fluent method chaining
         */
        public AmazonDynamoDBLockClientOptionsBuilder withCreateHeartbeatBackgroundThread(final Boolean createHeartbeatBackgroundThread) {
            this.createHeartbeatBackgroundThread = createHeartbeatBackgroundThread;
            return this;
        }

        /**
         * This parameter should be set to true only in the applications which do not have strict locking requirements.
         * When this is set to true, on DynamoDB service unavailable errors it is possible that two different clients can hold the lock.
         *
         * When heartbeat fails for lease duration period, the lock expires. If this parameter is set to true, and if a heartbeat
         * receives AmazonServiceException with a status code of HttpStatus.SC_SERVICE_UNAVAILABLE(503), the lock client will assume
         * that the heartbeat was a success and update the local state accordingly and it will keep holding the lock.
         *
         * @param holdLockOnServiceUnavailable Whether or not to hold the lock if DynamoDB Service is unavailable
         * @return a reference to this builder for fluent method chaining
         */
        public AmazonDynamoDBLockClientOptionsBuilder withHoldLockOnServiceUnavailable(final Boolean holdLockOnServiceUnavailable) {
            this.holdLockOnServiceUnavailable = holdLockOnServiceUnavailable;
            return this;
        }

        /**
         * Builds an AmazonDynamoDBLockClientOptions. If required parametes are
         * not set, will throw an IllegalArgumentsException.
         *
         * @return Returns the AmazonDynamoDBLockClientOptions which can be
         * passed to the lock client constructor.
         */
        public AmazonDynamoDBLockClientOptions build() {
            Objects.requireNonNull(this.tableName, "Table Name must not be null");
            Objects.requireNonNull(this.ownerName, "Owner Name must not be null");
            return new AmazonDynamoDBLockClientOptions(this.dynamoDBClient, this.tableName, this.partitionKeyName, this.sortKeyName, this.ownerName, this.leaseDuration,
                this.heartbeatPeriod, this.timeUnit, this.createHeartbeatBackgroundThread, this.namedThreadCreator, this.holdLockOnServiceUnavailable);
        }

        @Override
        public String toString() {
            return "AmazonDynamoDBLockClientOptionsBuilder(dynamoDBClient=" + this.dynamoDBClient + ", tableName=" + this.tableName + ", partitionKeyName=" + this.partitionKeyName
                + ", sortKeyName=" + this.sortKeyName + ", ownerName=" + this.ownerName + ", leaseDuration=" + this.leaseDuration + ", heartbeatPeriod=" + this.heartbeatPeriod
                + ", timeUnit=" + this.timeUnit + ", createHeartbeatBackgroundThread=" + this.createHeartbeatBackgroundThread
                + ", holdLockOnServiceUnavailable=" + this.holdLockOnServiceUnavailable + ")";
        }
    }

    /**
     * Creates an AmazonDynamoDBLockClientOptions builder object, which can be
     * used to create an AmazonDynamoDBLockClient. The only required parameters
     * are the client and the table name.
     *
     * @param dynamoDBClient The client for talking to DynamoDB.
     * @param tableName      The table containing the lock client.
     * @return A builder which can be used for creating a lock client.
     */
    public static AmazonDynamoDBLockClientOptionsBuilder builder(final AmazonDynamoDB dynamoDBClient, final String tableName) {
        return new AmazonDynamoDBLockClientOptionsBuilder(dynamoDBClient, tableName);
    }

    private AmazonDynamoDBLockClientOptions(final AmazonDynamoDB dynamoDBClient, final String tableName, final String partitionKeyName, final Optional<String> sortKeyName,
        final String ownerName, final Long leaseDuration, final Long heartbeatPeriod, final TimeUnit timeUnit, final Boolean createHeartbeatBackgroundThread,
        final Function<String, ThreadFactory> namedThreadCreator, final Boolean holdLockOnServiceUnavailable) {
        this.dynamoDBClient = dynamoDBClient;
        this.tableName = tableName;
        this.partitionKeyName = partitionKeyName;
        this.sortKeyName = sortKeyName;
        this.ownerName = ownerName;
        this.leaseDuration = leaseDuration;
        this.heartbeatPeriod = heartbeatPeriod;
        this.timeUnit = timeUnit;
        this.createHeartbeatBackgroundThread = createHeartbeatBackgroundThread;
        this.namedThreadCreator = namedThreadCreator;
        this.holdLockOnServiceUnavailable = holdLockOnServiceUnavailable;
    }

    /**
     * @return DynamoDB client that the lock client will use.
     */
    AmazonDynamoDB getDynamoDBClient() {
        return this.dynamoDBClient;
    }

    /**
     * @return The DynamoDB table name.
     */
    String getTableName() {
        return this.tableName;
    }

    /**
     * @return The partition key name within the table.
     */
    String getPartitionKeyName() {
        return this.partitionKeyName;
    }

    /**
     * @return sortKeyName
     * The sort key name. If this is set to Optional.absent(), then it means that the table only uses a partition key.
     */
    Optional<String> getSortKeyName() {
        return this.sortKeyName;
    }

    /**
     * @return The person that is acquiring the lock (for example, box.amazon.com.)
     */
    String getOwnerName() {
        return this.ownerName;
    }

    /**
     * @return The length of time that the lease for the lock will be granted
     * for. If this is set to, for example, 30 seconds, then the lock
     * will expire if the heartbeat is not sent for at least 30 seconds
     * (which would happen if the box or the heartbeat thread dies, for
     * example.)
     */
    Long getLeaseDuration() {
        return this.leaseDuration;
    }

    /**
     * @return How often the lock client will update DynamoDB to note that the
     * instance is still running.
     */
    Long getHeartbeatPeriod() {
        return this.heartbeatPeriod;
    }

    /**
     * @return The TimeUnit that is used for all time values in this object.
     */
    TimeUnit getTimeUnit() {
        return this.timeUnit;
    }

    /**
     * @return Whether or not the Lock Client will create a background thread.
     */
    Boolean getCreateHeartbeatBackgroundThread() {
        return this.createHeartbeatBackgroundThread;
    }

    /**
     * @return A function that takes in a thread name and outputs a ThreadFactory that creates threads with the given name.
     */
    Function<String, ThreadFactory> getNamedThreadCreator() {
        return this.namedThreadCreator;
    }

    /**
     * @return Whether or not to hold the lock if DynamoDB Service is unavailable
     */
    Boolean getHoldLockOnServiceUnavailable() {
        return this.holdLockOnServiceUnavailable;
    }
}