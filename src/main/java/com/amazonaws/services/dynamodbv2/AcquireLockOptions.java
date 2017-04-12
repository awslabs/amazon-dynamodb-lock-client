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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.amazonaws.metrics.RequestMetricCollector;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;

/**
 * Provides options for acquiring a lock when calling the acquireLock() method.
 *
 * @author <a href="mailto:dyanacek@amazon.com">David Yanacek</a>, <a href="mailto:slutsker@amazon.com">Sasha Slutsker</a>
 */
public class AcquireLockOptions {
    private final String partitionKey;
    private final Optional<String> sortKey;
    private final Optional<ByteBuffer> data;
    private final Boolean replaceData;
    private final Boolean deleteLockOnRelease;
    private final Long refreshPeriod;
    private final Long additionalTimeToWaitForLock;
    private final TimeUnit timeUnit;
    private final Map<String, AttributeValue> additionalAttributes;
    private final Optional<SessionMonitor> sessionMonitor;
    private final Optional<RequestMetricCollector> requestMetricCollector;


    /**
     * A builder for setting up an AcquireLockOptions object. This allows clients to configure
     * their settings when calling acquireLock(). The only required parameter is the partitionKey for
     * the lock.
     */
    public static class AcquireLockOptionsBuilder {
        private String partitionKey;
        private Optional<String> sortKey;
        private Optional<ByteBuffer> data;
        private Boolean replaceData;
        private Boolean deleteLockOnRelease;
        private Long refreshPeriod;
        private Long additionalTimeToWaitForLock;
        private TimeUnit timeUnit;
        private Map<String, AttributeValue> additionalAttributes;
        private Optional<RequestMetricCollector> requestMetricCollector;

        private long safeTimeWithoutHeartbeat;
        private Optional<Runnable> sessionMonitorCallback;
        private boolean isSessionMonitorSet = false;

        AcquireLockOptionsBuilder(final String partitionKey) {
            this.partitionKey = partitionKey;
            this.additionalAttributes = new HashMap<>();
            this.sortKey = Optional.empty();
            this.requestMetricCollector = Optional.empty();
            this.data = Optional.empty();
            this.replaceData = true;
            this.deleteLockOnRelease = true;
        }

        /**
         * @param sortKey The sort key to try and acquire the lock on (specify if
         *                and only if the table has sort keys.)
         * @return a reference to this builder for fluent method chaining
         */
        public AcquireLockOptionsBuilder withSortKey(final String sortKey) {
            this.sortKey = Optional.of(sortKey);
            return this;
        }

        /**
         * Sets data to be stored alongside the lock
         *
         * @param data Any data that needs to be stored alongside the lock (can
         *             be null if no data is needed to be stored there. If null
         *             with replaceData = true, the data will be removed)
         * @return a reference to this builder for fluent method chaining
         */
        public AcquireLockOptionsBuilder withData(final ByteBuffer data) {
            this.data = Optional.ofNullable(data);
            return this;
        }

        /**
         * Sets whether or not to replace any existing lock data with the data
         * parameter.
         *
         * @param replaceData If true, this will replace the lock data currently stored
         *                    alongside the lock.
         * @return a reference to this builder for fluent method chaining
         */
        public AcquireLockOptionsBuilder withReplaceData(final Boolean replaceData) {
            this.replaceData = replaceData;
            return this;
        }

        /**
         * @param deleteLockOnRelease Whether or not the lock should be deleted when close() is
         *                            called on the resulting LockItem
         * @return a reference to this builder for fluent method chaining
         */
        public AcquireLockOptionsBuilder withDeleteLockOnRelease(final Boolean deleteLockOnRelease) {
            this.deleteLockOnRelease = deleteLockOnRelease;
            return this;
        }

        /**
         * @param refreshPeriod How long to wait before trying to get the lock again (if
         *                      set to 10 seconds, for example, it would attempt to do so
         *                      every 10 seconds). If set, TimeUnit must also be set.
         * @return a reference to this builder for fluent method chaining
         */
        public AcquireLockOptionsBuilder withRefreshPeriod(final Long refreshPeriod) {
            this.refreshPeriod = refreshPeriod;
            return this;
        }

        /**
         * @param additionalTimeToWaitForLock How long to wait in addition to the lease duration (if set
         *                                    to 10 minutes, this will try to acquire a lock for at
         *                                    least 10 minutes before giving up and throwing the
         *                                    exception). If set, TimeUnit must also be set.
         * @return a reference to this builder for fluent method chaining
         */
        public AcquireLockOptionsBuilder withAdditionalTimeToWaitForLock(final Long additionalTimeToWaitForLock) {
            this.additionalTimeToWaitForLock = additionalTimeToWaitForLock;
            return this;
        }

        /**
         * @param timeUnit Sets the time unit for all time parameters set to non-null
         *                 in this bean
         * @return a reference to this builder for fluent method chaining
         */
        public AcquireLockOptionsBuilder withTimeUnit(final TimeUnit timeUnit) {
            this.timeUnit = timeUnit;
            return this;
        }

        /**
         * Stores some additional attributes with each lock. This can be used to add any arbitrary parameters to each lock row.
         *
         * @param additionalAttributes an arbitrary map of attributes to store with the lock row to be acquired
         * @return a reference to this builder for fluent method chaining
         */
        public AcquireLockOptionsBuilder withAdditionalAttributes(final Map<String, AttributeValue> additionalAttributes) {
            this.additionalAttributes = additionalAttributes;
            return this;
        }

        /**
         * <p>
         * Registers a "SessionMonitor."
         * </p>
         * <p>
         * The purpose of this SessionMonitor is to provide two abilities: provide
         * the ability to determine if the lock is about to expire, and run a
         * user-provided callback when the lock is about to expire. The advantage
         * this provides is notification that your lock is about to expire before it
         * is actually expired, and in case of leader election will help in
         * preventing that there are no two leaders present simultaneously.
         * </p>
         * <p>
         * If due to any reason heartbeating is unsuccessful for a configurable
         * period of time, your lock enters into a phase known as "danger zone." It
         * is during this "danger zone" that the callback will be run. It is also
         * during this period that a call to lockItem.amIAboutToExpire() will return
         * <code>true</code> and your application can take appropriate measures.
         * </p>
         * <p>
         * Bear in mind that the callback on a SessionMonitor may be null. In this
         * case, no callback will be run upon the lock entering the "danger zone";
         * yet, one can still make use of the amIAboutToExpire() method.
         * Furthermore, non-null callbacks can only ever be executed once in a
         * lock's lifetime. Independent of whether or not a callback is run, the
         * client will attempt to heartbeat the lock until the lock is released or
         * obtained by someone else.
         * </p>
         * <p>
         * Consider an example which uses this mechanism for leader election. One
         * way to make use of this SessionMonitor is to register a callback that
         * kills the JVM in case the leader's lock enters the danger zone:
         * </p>
         * <pre>
         * {
         *  final Runnable killJVM = new Runnable() {
         *      public void run() {
         *          Runtime.getRuntime().halt(1);
         *    }
         *  };
         *  try {
         *      final AcquireLockOptions options = AcquireLockOptions.builder("myLock")
         *          .withSessionMonitor(5L, killJVM)
         *          .withTimeUnit(TimeUnit.MINUTES)
         *          .build();
         *      final LockItem leaderLock = lockClient.acquireLock(options);
         *      goRunLeaderProcedures(); // safely run code knowing that after at least 5 minutes without heartbeating, the JVM will crash
         *  } catch (...) {
         *      ... //handle errors
         *  }
         * }
         * </pre>
         *
         * @param safeTimeWithoutHeartbeat the amount of time the lock can go without heartbeating before
         *                                 the lock is declared to be in the "danger zone"
         * @param sessionMonitorCallback   the callback to run when the lock's lease enters the danger
         *                                 zone
         * @return a reference to this builder for fluent method chaining
         */

        public AcquireLockOptionsBuilder withSessionMonitor(final long safeTimeWithoutHeartbeat, final Optional<Runnable> sessionMonitorCallback) {
            this.safeTimeWithoutHeartbeat = safeTimeWithoutHeartbeat;
            this.sessionMonitorCallback = sessionMonitorCallback;
            this.isSessionMonitorSet = true;
            return this;
        }

        /**
         * @param requestMetricCollector The request level metric collector to use; takes precedence over the ones at the
         *                               http client level and AWS SDK level.
         * @return a reference to this builder for fluent method chaining
         */
        public AcquireLockOptionsBuilder withRequestMetricCollector(final RequestMetricCollector requestMetricCollector) {
            this.requestMetricCollector = Optional.ofNullable(requestMetricCollector);
            return this;
        }

        public AcquireLockOptions build() {
            final Optional<SessionMonitor> sessionMonitor;
            if (this.isSessionMonitorSet) {
                Objects.requireNonNull(this.timeUnit, "timeUnit must not be null if sessionMonitor is non-null");
                sessionMonitor = Optional.of(new SessionMonitor(this.timeUnit.toMillis(this.safeTimeWithoutHeartbeat), this.sessionMonitorCallback));
            } else {
                sessionMonitor = Optional.empty();
            }
            return new AcquireLockOptions(this.partitionKey, this.sortKey, this.data, this.replaceData, this.deleteLockOnRelease, this.refreshPeriod,
                this.additionalTimeToWaitForLock, this.timeUnit, this.additionalAttributes, sessionMonitor, this.requestMetricCollector);
        }

        @Override
        public String toString() {
            return "AcquireLockOptions.AcquireLockOptionsBuilder(key=" + this.partitionKey + ", sortKey=" + this.sortKey + ", data=" + this.data + ", replaceData="
                + this.replaceData + ", deleteLockOnRelease=" + this.deleteLockOnRelease + ", refreshPeriod=" + this.refreshPeriod + ", additionalTimeToWaitForLock="
                + this.additionalTimeToWaitForLock + ", timeUnit=" + this.timeUnit + ", additionalAttributes=" + this.additionalAttributes + ", safeTimeWithoutHeartbeat="
                + this.safeTimeWithoutHeartbeat + ", sessionMonitorCallback=" + this.sessionMonitorCallback + ", requestMetricCollector=" + this.requestMetricCollector + ")";
        }
    }

    /**
     * Creates a new version of AcquireLockOptionsBuilder using the only
     * required parameter, which is a specific partition key.
     *
     * @param partitionKey The partition key under which the lock will be acquired.
     * @return a builder for an AquireLockOptions object
     */
    public static AcquireLockOptionsBuilder builder(final String partitionKey) {
        return new AcquireLockOptionsBuilder(partitionKey);
    }

    private AcquireLockOptions(final String partitionKey, final Optional<String> sortKey, final Optional<ByteBuffer> data, final Boolean replaceData,
        final Boolean deleteLockOnRelease, final Long refreshPeriod, final Long additionalTimeToWaitForLock, final TimeUnit timeUnit,
        final Map<String, AttributeValue> additionalAttributes, final Optional<SessionMonitor> sessionMonitor,
        final Optional<RequestMetricCollector> requestMetricCollector) {
        this.partitionKey = partitionKey;
        this.sortKey = sortKey;
        this.data = data;
        this.replaceData = replaceData;
        this.deleteLockOnRelease = deleteLockOnRelease;
        this.refreshPeriod = refreshPeriod;
        this.additionalTimeToWaitForLock = additionalTimeToWaitForLock;
        this.timeUnit = timeUnit;
        this.additionalAttributes = additionalAttributes;
        this.sessionMonitor = sessionMonitor;
        this.requestMetricCollector = requestMetricCollector;
    }

    String getPartitionKey() {
        return this.partitionKey;
    }

    Optional<String> getSortKey() {
        return this.sortKey;
    }

    Optional<ByteBuffer> getData() {
        return this.data;
    }

    Boolean getReplaceData() {
        return this.replaceData;
    }

    Boolean getDeleteLockOnRelease() {
        return this.deleteLockOnRelease;
    }

    Long getRefreshPeriod() {
        return this.refreshPeriod;
    }

    Long getAdditionalTimeToWaitForLock() {
        return this.additionalTimeToWaitForLock;
    }

    TimeUnit getTimeUnit() {
        return this.timeUnit;
    }

    Map<String, AttributeValue> getAdditionalAttributes() {
        return this.additionalAttributes;
    }

    /**
     * Constructs a SessionMonitor object for LockItem instantiation
     *
     * @return <code>Optional.absent()</code> if no call to
     * {with,set}SessionMonitor was made, else a SessionMonitor object
     * with the desired properties
     */
    Optional<SessionMonitor> getSessionMonitor() {
        return this.sessionMonitor;
    }

    Optional<RequestMetricCollector> getRequestMetricCollector() {
        return this.requestMetricCollector;
    }

    @Override
    public boolean equals(final Object other) {
        if (other == null || !(other instanceof AcquireLockOptions)) {
            return false;
        }

        final AcquireLockOptions otherOptions = (AcquireLockOptions) other;
        return Objects.equals(this.partitionKey, otherOptions.partitionKey)
                && Objects.equals(this.sortKey, otherOptions.sortKey)
                && Objects.equals(this.data, otherOptions.data)
                && Objects.equals(this.replaceData, otherOptions.replaceData)
                && Objects.equals(this.deleteLockOnRelease, otherOptions.deleteLockOnRelease)
                && Objects.equals(this.refreshPeriod, otherOptions.refreshPeriod)
                && Objects.equals(this.additionalTimeToWaitForLock, otherOptions.additionalTimeToWaitForLock)
                && Objects.equals(this.timeUnit, otherOptions.timeUnit)
                && Objects.equals(this.additionalAttributes, otherOptions.additionalAttributes)
                && Objects.equals(this.sessionMonitor, otherOptions.sessionMonitor)
                && Objects.equals(this.requestMetricCollector, otherOptions.requestMetricCollector);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.partitionKey, this.sortKey, this.data, this.replaceData, this.deleteLockOnRelease,
                this.refreshPeriod, this.additionalTimeToWaitForLock, this.timeUnit, this.additionalAttributes,
                this.sessionMonitor, this.requestMetricCollector);
    }
}

