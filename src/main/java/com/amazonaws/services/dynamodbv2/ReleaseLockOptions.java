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

/**
 * Provides options for releasing a lock when calling the releaseLock() method.
 * This class contains the options that may be configured during the act of releasing a lock.
 *
 * @author <a href="mailto:slutsker@amazon.com">Sasha Slutsker</a>
 */
public class ReleaseLockOptions {
    private final LockItem lockItem;
    private final boolean deleteLock;
    private final boolean bestEffort;
    private final Optional<ByteBuffer> data;

    ReleaseLockOptions(final LockItem lockItem, final boolean deleteLock, final boolean bestEffort, final Optional<ByteBuffer> data) {
        this.lockItem = lockItem;
        this.deleteLock = deleteLock;
        this.bestEffort = bestEffort;
        this.data = data;
    }

    public static class ReleaseLockOptionsBuilder {
        private LockItem lockItem;
        private boolean deleteLock;
        private boolean bestEffort;
        private Optional<ByteBuffer> data;

        ReleaseLockOptionsBuilder(final LockItem lockItem) {
            this.lockItem = lockItem;
            this.deleteLock = true;
            this.bestEffort = false;
            this.data = Optional.empty();
        }

        /**
         * Whether or not to delete the lock when releasing it. If set to false, the
         * lock row will continue to be in DynamoDB, but it will be marked as
         * released.
         *
         * @param deleteLock true to delete the lock when releasing, false otherwise
         * @return a reference to this builder for fluent method chaining
         */
        public ReleaseLockOptionsBuilder withDeleteLock(final boolean deleteLock) {
            this.deleteLock = deleteLock;
            return this;
        }

        /**
         * Whether or not to ignore {@code AmazonClientException} when releasing the
         * lock. If set to true, any exception when calling DynamoDB will be ignored
         * and the clean up steps will continue, hence the lock item in DynamoDb
         * might not be updated / deleted but will eventually expire.
         *
         * @param bestEffort true to ignore {@code AmazonClientException} when releasing,
         *                   false otherwise
         * @return a reference to this builder for fluent method chaining
         */
        public ReleaseLockOptionsBuilder withBestEffort(final boolean bestEffort) {
            this.bestEffort = bestEffort;
            return this;
        }

        /**
         * New data to persist to the lock (only used if deleteLock=false.) If the
         * data is null, then the lock client will keep the data as-is and not
         * change it.
         *
         * @param data The data to persist when deleting the lock
         * @return a reference to this builder for fluent method chaining
         */
        public ReleaseLockOptionsBuilder withData(final ByteBuffer data) {
            this.data = Optional.ofNullable(data);
            return this;
        }

        public ReleaseLockOptions build() {
            return new ReleaseLockOptions(this.lockItem, this.deleteLock, this.bestEffort, this.data);
        }

        @Override
        public java.lang.String toString() {
            return "ReleaseLockOptions.ReleaseLockOptionsBuilder(lockItem=" + this.lockItem + ", deleteLock=" + this.deleteLock + ", bestEffort=" + this.bestEffort + ", data="
                + this.data + ")";
        }
    }

    /**
     * Creates a builder for the ReleaseLockOptions object. The only required
     * parameter is lockItem. The rest are defaulted, such as deleting the lock
     * on release is set to true and best effort is set to false. The builder
     * can be used to customize these parameters.
     *
     * @param lockItem The lock item to release.
     * @return a builder of ReleaseLockOptions instances
     */
    public static ReleaseLockOptionsBuilder builder(final LockItem lockItem) {
        return new ReleaseLockOptionsBuilder(lockItem);
    }

    LockItem getLockItem() {
        return this.lockItem;
    }

    boolean isDeleteLock() {
        return this.deleteLock;
    }

    boolean isBestEffort() {
        return this.bestEffort;
    }

    Optional<ByteBuffer> getData() {
        return this.data;
    }
}