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

import java.util.Optional;
import java.util.concurrent.ThreadFactory;

import com.amazonaws.services.dynamodbv2.util.LockClientUtils;

/**
 * A class that handles the information regarding a lock's lease entering a
 * "danger zone," or alarming period of time before the lock's lease expires.
 * <p>
 * See {@code AcquireLockOptions} for more details on "danger zone"
 *
 * @author <a href="mailto:joserock@amazon.com">Joseph Rock</a>
 */
final class SessionMonitor {
    private final long safeTimeWithoutHeartbeatMillis;
    private final Optional<Runnable> callback;
    private final ThreadFactory threadFactory;

    /**
     * Constructs a SessionMonitor object.
     *
     * @param safeTimeWithoutHeartbeatMillis the amount of time (in milliseconds) the lock can go without
     *                                       heartbeating before the lock is declared to be in the
     *                                       "danger zone"
     * @param callback                       the callback to run when the lock's lease enters the danger
     *                                       zone
     * @param threadFactory                  the factory to create the thread that will run the callback
     */
    public SessionMonitor(
            final long safeTimeWithoutHeartbeatMillis,
            final Optional<Runnable> callback,
            final ThreadFactory threadFactory) {
        this.safeTimeWithoutHeartbeatMillis = safeTimeWithoutHeartbeatMillis;
        this.callback = callback;
        this.threadFactory = threadFactory;
    }

    /**
     * Given the last time the lease was renewed, determines whether or not the
     * lease has entered the period in which the callback should be fired.
     *
     * @param lastAbsoluteTimeUpdatedMillis the last exact point in time that the lock's lease was renewed
     *                                      (in milliseconds)
     * @return <code>true</code> if the current time is greater than or equal to
     * the projected time at which the lease is said to go into the
     * "danger zone," <code>false</code> if otherwise
     */
    public boolean isLeaseEnteringDangerZone(final long lastAbsoluteTimeUpdatedMillis) {
        return this.millisecondsUntilLeaseEntersDangerZone(lastAbsoluteTimeUpdatedMillis) <= 0;
    }

    /**
     * Given the last time the lease was renewed, determines the number of
     * milliseconds until the lease will enter the period in which the callback
     * should be fired
     *
     * @param lastAbsoluteTimeUpdatedMillis the last exact point in time that the lock's lease was renewed
     *                                      (in milliseconds)
     * @return number of milliseconds until the lease will enter the "danger
     * zone" if no more heartbeats are sent. May be negative if that
     * time has already passed.
     */
    long millisecondsUntilLeaseEntersDangerZone(final long lastAbsoluteTimeUpdatedMillis) {
        return lastAbsoluteTimeUpdatedMillis + this.safeTimeWithoutHeartbeatMillis - LockClientUtils.INSTANCE.millisecondTime();
    }

    /**
     * Creates a daemon thread which will run the callback.
     */
    public void runCallback() {
        if (this.callback.isPresent()) {
            final Thread t = this.threadFactory.newThread(this.callback.get());
            t.setDaemon(true);
            t.start();
        }
    }

    /**
     * Returns whether or not the callback is non-null
     *
     * @return <code>true</code> if this SessionMonitor has a callback,
     * otherwise <code>false</code>
     */
    public boolean hasCallback() {
        return this.callback != null;
    }

    /**
     * Returns the safe time without heartbeating in milliseconds
     */
    public long getSafeTimeMillis() {
        return this.safeTimeWithoutHeartbeatMillis;
    }
}
