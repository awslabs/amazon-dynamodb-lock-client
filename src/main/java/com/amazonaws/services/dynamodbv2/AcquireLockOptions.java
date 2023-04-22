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
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Provides options for acquiring a lock when calling the acquireLock() method.
 *
 * @author <a href="mailto:dyanacek@amazon.com">David Yanacek</a>
 * @author <a href="mailto:slutsker@amazon.com">Sasha Slutsker</a>
 * @author <a href="mailto:amcp@amazon.com">Alexander Patrikalakis</a>
 */
public class AcquireLockOptions {
    private final String partitionKey;
    private final Optional<String> sortKey;
    private final Optional<ByteBuffer> data;
    private final Boolean replaceData;
    private final Boolean deleteLockOnRelease;
    private final Boolean acquireOnlyIfLockAlreadyExists;
    private final Long refreshPeriod;
    private final Long additionalTimeToWaitForLock;
    private final TimeUnit timeUnit;
    private final Map<String, AttributeValue> additionalAttributes;
    private final Boolean updateExistingLockRecord;
    private final Boolean acquireReleasedLocksConsistently;
    private final Optional<SessionMonitor> sessionMonitor;
    private final Boolean reentrant;

    /**
     * Setting this flag to true will prevent the thread from being blocked (put to sleep) for the lease duration and
     * instead will return the call with the lock not granted exception back to the caller. It is up to the caller to
     * optionally back-off and retry and to acquire the lock.
     */
    private final boolean shouldSkipBlockingWait;

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
        private Boolean acquireOnlyIfLockAlreadyExists;
        private Long refreshPeriod;
        private Long additionalTimeToWaitForLock;
        private TimeUnit timeUnit;
        private Map<String, AttributeValue> additionalAttributes;
        private Boolean updateExistingLockRecord;
        private Boolean acquireReleasedLocksConsistently;
        private Boolean reentrant;

        private long safeTimeWithoutHeartbeat;
        private Optional<Runnable> sessionMonitorCallback;
        private boolean isSessionMonitorSet = false;
        private boolean shouldSkipBlockingWait;

        AcquireLockOptionsBuilder(final String partitionKey) {
            this.partitionKey = partitionKey;
            this.additionalAttributes = new HashMap<>();
            this.sortKey = Optional.empty();
            this.data = Optional.empty();
            this.replaceData = true;
            this.deleteLockOnRelease = true;
            this.acquireOnlyIfLockAlreadyExists = false;
            this.updateExistingLockRecord = false;
            this.shouldSkipBlockingWait = false;
            this.acquireReleasedLocksConsistently = false;
            this.reentrant = false;
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
         * Sets whether or not to allow acquiring locks if the lock does not exist already
         *
         * @param acquireOnlyIfLockAlreadyExists If true, locks will not be granted on items which do not already
         *                                       exist.
         *                                       If false (default value), lock item will be created if it does not
         *                                       exist already.
         * @return a reference to this builder for fluent method chaining
         */
        public AcquireLockOptionsBuilder withAcquireOnlyIfLockAlreadyExists(final Boolean acquireOnlyIfLockAlreadyExists) {
            this.acquireOnlyIfLockAlreadyExists = acquireOnlyIfLockAlreadyExists;
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
         * With this being true lock client will only update the current lock record if present otherwise create a new one.
         *
         *
         * @param updateExistingLockRecord whether lock client should always create a new lock record and
         *                                removing any other entries not known to lock client
         * @return a reference to this builder for fluent method chaining
         */
        public AcquireLockOptionsBuilder withUpdateExistingLockRecord(final Boolean updateExistingLockRecord) {
            this.updateExistingLockRecord = updateExistingLockRecord;
            return this;
        }

        /**
         * With this being true, the lock client will not block the running thread and wait for lock, rather will fast fail the request,
         * so that the caller can either choose to back-off and process the same request or start processing a new request.
         *
         *
         * @param shouldSkipBlockingWait whether lock client should skip the logic to block the thread if lock is owned by another machine
         *                                  and lock is still active.
         *
         * @return a reference to this builder for fluent method chaining
         */
        public AcquireLockOptionsBuilder withShouldSkipBlockingWait(final boolean shouldSkipBlockingWait) {
            this.shouldSkipBlockingWait = shouldSkipBlockingWait;
            return this;
        }

        /**
         * <p>
         * With this being true, the lock client will ensure that released locks are acquired consistently in order to preserve existing
         * lock data in dynamodb.
         * </p>
         *
         * <p>
         *  This option is currently disabled by default.  Will be enabled by default in the next major release.
         * </p>
         *
         * @param acquireReleasedLocksConsistently whether the lock client should acquire released locks consistently
         *
         * @return a reference to this builder for fluent method chaining
         */
        public AcquireLockOptionsBuilder withAcquireReleasedLocksConsistently(final boolean acquireReleasedLocksConsistently) {
            this.acquireReleasedLocksConsistently = acquireReleasedLocksConsistently;
            return this;
        }

        /**
         * With this set to true, the lock client will check first if it already owns the lock. If it already owns the lock and the
         * lock is not expired, it will return the lock immediately. If this is set to false and the client already owns the lock,
         * the call to acquireLock will block.
         *
         * @param reentrant whether the lock client should not block if it already owns the lock
         * @return a reference to this builder for fluent method chaining
         */
        public AcquireLockOptionsBuilder withReentrant(final boolean reentrant) {
            this.reentrant = reentrant;
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

        public AcquireLockOptions build() {
            final Optional<SessionMonitor> sessionMonitor;
            if (this.isSessionMonitorSet) {
                Objects.requireNonNull(this.timeUnit, "timeUnit must not be null if sessionMonitor is non-null");
                sessionMonitor = Optional.of(new SessionMonitor(this.timeUnit.toMillis(this.safeTimeWithoutHeartbeat), this.sessionMonitorCallback));
            } else {
                sessionMonitor = Optional.empty();
            }
            return new AcquireLockOptions(this.partitionKey, this.sortKey, this.data, this.replaceData, this.deleteLockOnRelease, this.acquireOnlyIfLockAlreadyExists,
                    this.refreshPeriod, this.additionalTimeToWaitForLock, this.timeUnit, this.additionalAttributes, sessionMonitor,
                    this.updateExistingLockRecord, this.shouldSkipBlockingWait, this.acquireReleasedLocksConsistently, this.reentrant);
        }

        @Override
        public String toString() {
            return "AcquireLockOptions.AcquireLockOptionsBuilder(key=" + this.partitionKey + ", sortKey=" + this.sortKey + ", data=" + this.data + ", replaceData="
                + this.replaceData + ", deleteLockOnRelease=" + this.deleteLockOnRelease + ", refreshPeriod=" + this.refreshPeriod + ", additionalTimeToWaitForLock="
                + this.additionalTimeToWaitForLock + ", timeUnit=" + this.timeUnit + ", additionalAttributes=" + this.additionalAttributes + ", safeTimeWithoutHeartbeat="
                + this.safeTimeWithoutHeartbeat + ", sessionMonitorCallback=" + this.sessionMonitorCallback + ", acquireReleasedLocksConsistently="
                + this.acquireReleasedLocksConsistently + ", reentrant=" + this.reentrant+ ")";
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
       final Boolean deleteLockOnRelease, final Boolean acquireOnlyIfLockAlreadyExists, final Long refreshPeriod, final Long additionalTimeToWaitForLock,
       final TimeUnit timeUnit, final Map<String, AttributeValue> additionalAttributes, final Optional<SessionMonitor> sessionMonitor,
       final Boolean updateExistingLockRecord, final Boolean shouldSkipBlockingWait, final Boolean acquireReleasedLocksConsistently, Boolean reentrant) {
       this.partitionKey = partitionKey;
       this.sortKey = sortKey;
       this.data = data;
       this.replaceData = replaceData;
       this.deleteLockOnRelease = deleteLockOnRelease;
       this.acquireOnlyIfLockAlreadyExists = acquireOnlyIfLockAlreadyExists;
       this.refreshPeriod = refreshPeriod;
       this.additionalTimeToWaitForLock = additionalTimeToWaitForLock;
       this.timeUnit = timeUnit;
       this.additionalAttributes = additionalAttributes;
       this.sessionMonitor = sessionMonitor;
       this.updateExistingLockRecord = updateExistingLockRecord;
       this.shouldSkipBlockingWait = shouldSkipBlockingWait;
       this.acquireReleasedLocksConsistently = acquireReleasedLocksConsistently;
       this.reentrant = reentrant;
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

    Boolean getUpdateExistingLockRecord() { return this.updateExistingLockRecord; }

    Boolean getAcquireReleasedLocksConsistently() {
        return this.acquireReleasedLocksConsistently;
    }

    Boolean getAcquireOnlyIfLockAlreadyExists() {
        return this.acquireOnlyIfLockAlreadyExists;
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

    Boolean getReentrant() {
      return this.reentrant;
    }

    Map<String, AttributeValue> getAdditionalAttributes() {
        return this.additionalAttributes;
    }

    /**
     * Constructs a SessionMonitor object for LockItem instantiation
     *
     * @return <code>Optional.empty()</code> if no call to
     * {with,set}SessionMonitor was made, else a SessionMonitor object
     * with the desired properties
     */
    Optional<SessionMonitor> getSessionMonitor() {
        return this.sessionMonitor;
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
                && Objects.equals(this.acquireOnlyIfLockAlreadyExists, otherOptions.acquireOnlyIfLockAlreadyExists)
                && Objects.equals(this.refreshPeriod, otherOptions.refreshPeriod)
                && Objects.equals(this.additionalTimeToWaitForLock, otherOptions.additionalTimeToWaitForLock)
                && Objects.equals(this.timeUnit, otherOptions.timeUnit)
                && Objects.equals(this.additionalAttributes, otherOptions.additionalAttributes)
                && Objects.equals(this.sessionMonitor, otherOptions.sessionMonitor)
                && Objects.equals(this.updateExistingLockRecord, otherOptions.updateExistingLockRecord)
                && Objects.equals(this.shouldSkipBlockingWait, otherOptions.shouldSkipBlockingWait)
                && Objects.equals(this.acquireReleasedLocksConsistently, otherOptions.acquireReleasedLocksConsistently)
                && Objects.equals(this.reentrant, otherOptions.reentrant);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.partitionKey, this.sortKey, this.data, this.replaceData, this.deleteLockOnRelease,
                this.acquireOnlyIfLockAlreadyExists, this.refreshPeriod, this.additionalTimeToWaitForLock, this.timeUnit,
                this.additionalAttributes, this.sessionMonitor, this.updateExistingLockRecord,
                this.shouldSkipBlockingWait, this.acquireReleasedLocksConsistently, this.reentrant);

    }

    public boolean shouldSkipBlockingWait() {
        return shouldSkipBlockingWait;
    }
}

