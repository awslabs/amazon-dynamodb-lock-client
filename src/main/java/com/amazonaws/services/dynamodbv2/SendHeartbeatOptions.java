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
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * A simple bean for sending lock heartbeats or updating the lock data with various combinations of overrides of the default
 * behavior. This bean avoids having to make every combination of override to the sendHeartbeat() method, or to have the user pass
 * specify every argument.
 *
 * @author <a href="mailto:dyanacek@amazon.com">David Yanacek</a>
 */
public class SendHeartbeatOptions {
    private final LockItem lockItem;
    private final Optional<ByteBuffer> data;
    private final Boolean deleteData;
    private final Long leaseDurationToEnsure;
    private final TimeUnit timeUnit;

    private SendHeartbeatOptions(final LockItem lockItem, final Optional<ByteBuffer> data, final Boolean deleteData, final Long leaseDurationToEnsure, final TimeUnit timeUnit) {
        this.lockItem = lockItem;
        this.data = data;
        this.deleteData = deleteData;
        this.leaseDurationToEnsure = leaseDurationToEnsure;
        this.timeUnit = timeUnit;
    }

    public static class SendHeartbeatOptionsBuilder {
        private LockItem lockItem;
        private Optional<ByteBuffer> data;
        private Boolean deleteData;
        private Long leaseDurationToEnsure;
        private TimeUnit timeUnit;

        SendHeartbeatOptionsBuilder(final LockItem lockItem) {
            this.lockItem = lockItem;
            this.data = Optional.empty();
        }

        /**
         * @param data New data to update in the lock item. If null, will delete the data from the item.
         * @return a reference to this builder for fluent method chaining
         */
        public SendHeartbeatOptionsBuilder withData(final ByteBuffer data) {
            this.data = Optional.ofNullable(data);
            return this;
        }

        /**
         * @param deleteData True if the extra data associated with the lock should be deleted. Must be null or false if data is non-null.
         *                   Defaults to false.
         * @return a reference to this builder for fluent method chaining
         */
        public SendHeartbeatOptionsBuilder withDeleteData(final Boolean deleteData) {
            this.deleteData = deleteData;
            return this;
        }

        /**
         * @param leaseDurationToEnsure The new duration of the lease after the heartbeat is sent.
         * @return a reference to this builder for fluent method chaining
         */
        public SendHeartbeatOptionsBuilder withLeaseDurationToEnsure(final Long leaseDurationToEnsure) {
            this.leaseDurationToEnsure = leaseDurationToEnsure;
            return this;
        }

        /**
         * @param timeUnit The time unit for all time parameters in this bean
         * @return a reference to this builder for fluent method chaining
         */
        public SendHeartbeatOptionsBuilder withTimeUnit(final TimeUnit timeUnit) {
            this.timeUnit = timeUnit;
            return this;
        }

        public SendHeartbeatOptions build() {
            return new SendHeartbeatOptions(this.lockItem, this.data, this.deleteData, this.leaseDurationToEnsure, this.timeUnit);
        }

        @Override
        public java.lang.String toString() {
            return "SendHeartbeatOptions.SendHeartbeatOptionsBuilder(lockItem=" + this.lockItem + ", data=" + this.data + ", deleteData=" + this.deleteData
                + ", leaseDurationToEnsure=" + this.leaseDurationToEnsure + ", timeUnit=" + this.timeUnit + ")";
        }
    }

    /**
     * Creates a SendHeartbeatOptionsBuilder, which can be used for setting
     * options for the sendHeartbeat() method in the lock client. The only
     * required parameter is specified here, which is the lockItem.
     *
     * @param lockItem The lock to send a heartbeat for.
     * @return a builder for SendHeartbeatOptions
     */
    public static SendHeartbeatOptionsBuilder builder(final LockItem lockItem) {
        return new SendHeartbeatOptionsBuilder(lockItem);
    }

    LockItem getLockItem() {
        return this.lockItem;
    }

    Optional<ByteBuffer> getData() {
        return this.data;
    }

    Boolean getDeleteData() {
        return this.deleteData;
    }

    Long getLeaseDurationToEnsure() {
        return this.leaseDurationToEnsure;
    }

    TimeUnit getTimeUnit() {
        return this.timeUnit;
    }
}