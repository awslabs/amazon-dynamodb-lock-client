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

import com.amazonaws.services.dynamodbv2.model.LockNotGrantedException;
import com.amazonaws.services.dynamodbv2.model.SessionMonitorNotSetException;
import com.amazonaws.services.dynamodbv2.util.LockClientUtils;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * A lock that has been successfully acquired. Provides APIs for releasing, heartbeating, and extending the lock.
 *
 * @author <a href="mailto:slutsker@amazon.com">Sasha Slutsker</a>
 */
public class LockItem implements Closeable {
    private final AmazonDynamoDBLockClient client;
    private final String partitionKey;
    private final Optional<String> sortKey;

    private Optional<ByteBuffer> data;
    private final String ownerName;
    private final boolean deleteLockItemOnClose;
    private final boolean isReleased;
    private final AtomicLong lookupTime;
    private final StringBuffer recordVersionNumber;
    private final AtomicLong leaseDuration;
    private final Map<String, AttributeValue> additionalAttributes;

    private final Optional<SessionMonitor> sessionMonitor;

    /**
     * Creates a lock item representing a given key. This constructor should
     * only be called by the lock client -- the caller should use the version of
     * LockItem returned by {@code acquireLock}. In order to enforce this, it is
     * package-private.
     *
     * @param client                        The AmazonDynamoDBLockClient object associated with this lock
     * @param partitionKey                  The key representing the lock
     * @param sortKey                       The sort key, if the DynamoDB table supports sort keys (or
     *                                      Optional.empty otherwise)
     * @param data                          The data stored in the lock (can be null)
     * @param deleteLockItemOnClose         Whether or not to delete the lock item when releasing it
     * @param ownerName                     The owner associated with the lock
     * @param leaseDuration                 How long the lease for the lock is (in milliseconds)
     * @param lastUpdatedTimeInMilliseconds How recently the lock was updated (in milliseconds)
     * @param recordVersionNumber           The current record version number of the lock -- this is
     *                                      globally unique and changes each time the lock is updated
     * @param isReleased                    Whether the item in DynamoDB is marked as released, but still
     *                                      exists in the table
     * @param sessionMonitor                Optionally, the SessionMonitor object with which to associate
     *                                      the lock
     * @param additionalAttributes          Additional attributes that can optionally be stored alongside
     *                                      the lock
     */
    LockItem(final AmazonDynamoDBLockClient client, final String partitionKey, final Optional<String> sortKey, final Optional<ByteBuffer> data, final boolean deleteLockItemOnClose,
        final String ownerName, final long leaseDuration, final long lastUpdatedTimeInMilliseconds, final String recordVersionNumber, final boolean isReleased,
        final Optional<SessionMonitor> sessionMonitor, final Map<String, AttributeValue> additionalAttributes) {
        Objects.requireNonNull(partitionKey, "Cannot create a lock with a null key");
        Objects.requireNonNull(ownerName, "Cannot create a lock with a null owner");
        Objects.requireNonNull(sortKey, "Cannot create a lock with a null sortKey (use Optional.empty())");
        Objects.requireNonNull(data, "Cannot create a lock with a null data (use Optional.empty())");
        this.client = client;
        this.partitionKey = partitionKey;
        this.sortKey = sortKey;
        this.data = data;
        this.ownerName = ownerName;
        this.deleteLockItemOnClose = deleteLockItemOnClose;

        this.leaseDuration = new AtomicLong(leaseDuration);
        this.lookupTime = new AtomicLong(lastUpdatedTimeInMilliseconds);
        this.recordVersionNumber = new StringBuffer(recordVersionNumber);
        this.isReleased = isReleased;
        this.sessionMonitor = sessionMonitor;
        this.additionalAttributes = additionalAttributes;
    }

    /**
     * Returns the key associated with this lock, which is unique for every lock (unless the lock has a sort key, in which case
     * key + sortKey is unique.)
     *
     * @return The key identifying the lock.
     */
    public String getPartitionKey() {
        return this.partitionKey;
    }

    /**
     * Returns the sort key associated with the lock, if there is one.
     *
     * @return The sort key for the lock.
     */
    public Optional<String> getSortKey() {
        return this.sortKey;
    }

    /**
     * @return Returns the data associated with the lock, which is optional.
     */
    public Optional<ByteBuffer> getData() {
        return this.data;
    }

    /**
     * Returns the additional attributes that can optionally be stored alongside the lock.
     *
     * @return The additional attributes that can optionally be stored alongside the lock.
     */
    public Map<String, AttributeValue> getAdditionalAttributes() {
        return this.additionalAttributes;
    }


    /**
     * Returns the name of the owner that owns this lock.
     *
     * @return The owner name
     */
    public String getOwnerName() {
        return this.ownerName;
    }

    /**
     * Returns the last time this lock was updated. Note that this will use LockClientUtils.millisecondTime() so it does not represent
     * an actual absolute time.
     *
     * @return The last time this lock was updated
     */
    public long getLookupTime() {
        return this.lookupTime.get();
    }

    /**
     * Returns the current record version number of the lock in DynamoDB. This is what tells the lock client when the lock is
     * stale.
     *
     * @return The current record version number
     */
    public String getRecordVersionNumber() {
        return this.recordVersionNumber.toString();
    }

    /**
     * Returns the amount of time that the client has this lock for, which can be kept up to date by calling
     * {@code sendHeartbeat}.
     *
     * @return the lease duration of this lock item
     */
    public long getLeaseDuration() {
        return this.leaseDuration.get();
    }

    /**
     * Returns a boolean indicating whether the lock should be deleted from DynamoDB after release.
     *
     * @return true if the lock should be deleted, false if it should not.
     */
    public boolean getDeleteLockItemOnClose() {
        return this.deleteLockItemOnClose;
    }

    /**
     * Releases the lock for others to use.
     */
    @Override
    public void close() {
        this.client.releaseLock(this);
    }

    /**
     * Returns a string representation of this lock.
     */
    @Override
    public String toString() {
        String dataString = this.data
            .map(byteBuffer -> new String(byteBuffer.array(), StandardCharsets.UTF_8))
            .orElse("");
        return String
            .format("LockItem{Partition Key=%s, Sort Key=%s, Owner Name=%s, Lookup Time=%d, Lease Duration=%d, "
                    + "Record Version Number=%s, Delete On Close=%s, Data=%s, Is Released=%s}",
                this.partitionKey, this.sortKey, this.ownerName, this.lookupTime.get(), this.leaseDuration.get(), this.recordVersionNumber, this.deleteLockItemOnClose,
                dataString, this.isReleased);
    }

    /**
     * Hash code of just the (key, ownerName) as that is what uniquely identifies this lock.
     */
    @Override
    public int hashCode() {
        return Arrays.hashCode(Arrays.asList(this.partitionKey, this.ownerName).toArray());
    }

    /**
     * Returns if two locks have the same (key, ownerName)
     */
    @Override
    public boolean equals(final Object other) {
        if (other == null || !(other instanceof LockItem)) {
            return false;
        }

        final LockItem otherLockItem = (LockItem) other;
        /* key and ownerName should never be null due to a preconditions check */
        return this.partitionKey.equals(otherLockItem.getPartitionKey()) && this.ownerName.equals(otherLockItem.getOwnerName());
    }

    /**
     * Returns whether or not the lock is expired, based on the lease duration and when the last heartbeat was sent.
     *
     * @return True if the lock is expired, false otherwise
     */
    public boolean isExpired() {
        if (this.isReleased) {
            return true;
        }
        return LockClientUtils.INSTANCE.millisecondTime() - this.lookupTime.get() > this.leaseDuration.get();
    }

    /**
     * Returns whether or not the lock was marked as released when loaded from DynamoDB. Does not consider expiration time.
     *
     * @return True if the lock was marked as released when loaded from DynamoDB
     */
    boolean isReleased() {
        return this.isReleased;
    }

    /**
     * Sends a heartbeat to indicate that the given lock is still being worked on. If using
     * {@code createHeartbeatBackgroundThread}=true when setting up this object, then this method is unnecessary, because the
     * background thread will be periodically calling it and sending heartbeats. However, if
     * {@code createHeartbeatBackgroundThread}=false, then this method must be called to instruct DynamoDB that the lock should
     * not be expired.
     * <p>
     * This is equivalent to calling lockClient.sendHeartbeat(lockItem)
     */
    public void sendHeartBeat() {
        this.client.sendHeartbeat(this);
    }

    /**
     * <p>
     * Ensures that this owner has the lock for a specified period of time. If the lock will expire in less than the amount of
     * time passed in, then this method will do nothing. Otherwise, it will set the {@code leaseDuration} to that value and send a
     * heartbeat, such that the lock will expire no sooner than after {@code leaseDuration} elapses.
     * </p>
     * <p>
     * This method is not required if using heartbeats, because the client could simply call {@code isExpired} before every
     * operation to ensure that it still has the lock. However, it is possible for the client to instead call this method before
     * executing every operation if they all require different lengths of time, and the client wants to ensure it always has
     * enough time.
     * </p>
     * <p>
     * This method will throw a {@code LockNotGrantedException} if it does not currently hold the lock.
     * </p>
     *
     * @param leaseDurationToEnsure The amount of time to ensure that the lease is granted for
     * @param timeUnit              The time unit for the leaseDuration
     */
    public void ensure(final long leaseDurationToEnsure, final TimeUnit timeUnit) {
        Objects.requireNonNull(timeUnit, "TimeUnit cannot be null");
        if (this.isReleased) {
            throw new LockNotGrantedException("Lock is released");
        }
        final long leaseDurationToEnsureInMilliseconds = timeUnit.toMillis(leaseDurationToEnsure);
        if (this.leaseDuration.get() - (LockClientUtils.INSTANCE.millisecondTime() - this.lookupTime.get()) <= leaseDurationToEnsureInMilliseconds) {
            this.client.sendHeartbeat(SendHeartbeatOptions.builder(this).withLeaseDurationToEnsure(leaseDurationToEnsure).withTimeUnit(timeUnit).build());
        }
    }

    /*
     * Updates the record version number of the lock. This method is package private -- it should only be called by the lock
     * client.
     */
    void updateRecordVersionNumber(final String recordVersionNumber, final long lastUpdateOfLock, final long leaseDurationToEnsureInMilliseconds) {
        this.recordVersionNumber.replace(0, recordVersionNumber.length(), recordVersionNumber);
        this.lookupTime.set(lastUpdateOfLock);
        this.leaseDuration.set(leaseDurationToEnsureInMilliseconds);
    }

    /*
     * Updates the data of the lock. This method is package private -- it should only be called by the lock
     * client.
     */
    void updateData(ByteBuffer byteBuffer) {
        this.data = Optional.ofNullable(byteBuffer);
    }

    /**
     * Updates the last updated time of the lock.
     *
     * @param lastUpdateOfLock - Time to update the lock with
     */
    public void updateLookUpTime(final long lastUpdateOfLock) {
        this.lookupTime.set(lastUpdateOfLock);
    }

    /*
     * Returns the unique identifier for the lock so it can be stored in a HashMap under that key
     */
    String getUniqueIdentifier() {
        return this.partitionKey + this.sortKey.orElse("");
    }

    /**
     * Returns whether or not the lock is entering the "danger zone" time
     * period.
     *
     * @return <code>true</code> if the lock has been released or the lock's
     * lease has entered the "danger zone" * <code>false</code> if the
     * lock has not been released and the lock has not yet entered the
     * "danger zone"
     * @throws SessionMonitorNotSetException when the SessionMonitor is not set
     * @throws IllegalStateException         when the lock is already released
     */
    public boolean amIAboutToExpire() {
        return this.millisecondsUntilDangerZoneEntered() <= 0;
    }

    /**
     * Returns the amount of time left before the lock enters the "danger zone"
     *
     * @return number of milliseconds before the lock enters the "danger zone"
     * if no heartbeats are sent for it, may be negative if it has
     * already passed
     * @throws SessionMonitorNotSetException when the SessionMonitor is not set
     * @throws IllegalStateException         when the lock is already released
     */
    long millisecondsUntilDangerZoneEntered() {
        if (!this.sessionMonitor.isPresent()) {
            throw new SessionMonitorNotSetException("SessionMonitor is not set");
        }
        if (this.isReleased) {
            throw new IllegalStateException("Lock is already released");
        }
        return this.sessionMonitor.get().millisecondsUntilLeaseEntersDangerZone(this.getLookupTime());
    }

    /**
     * Returns whether or not the lock has a SessionMonitor instance.
     *
     * @return <code>true</code> if the session monitor is set otherwise
     * <code>false</code>
     */
    boolean hasSessionMonitor() {
        return this.sessionMonitor.isPresent();
    }

    /**
     * Returns whether or not the lock's SessionMonitor has a callback
     *
     * @return <code>true</code> if the SessionMonitor has a callback, otherwise
     * <code>false</code>
     * @throws SessionMonitorNotSetException when the SessionMonitor is not set
     */
    boolean hasCallback() {
        if (!this.sessionMonitor.isPresent()) {
            throw new SessionMonitorNotSetException("SessionMonitor is not set");
        }
        return this.sessionMonitor.get().hasCallback();
    }

    /**
     * Calls runCallback() on the SessionMonitor object after checking the
     * SessionMonitor's existence.
     *
     * @throws SessionMonitorNotSetException if session monitor is not set
     */
    void runSessionMonitor() {
        if (!this.sessionMonitor.isPresent()) {
            throw new SessionMonitorNotSetException("Can't run callback without first setting SessionMonitor");
        }
        this.sessionMonitor.get().runCallback();
    }
}
