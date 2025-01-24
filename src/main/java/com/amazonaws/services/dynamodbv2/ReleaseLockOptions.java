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

import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
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
    private final Map<String, AttributeValueUpdate> additionalAttributeUpdates;

    ReleaseLockOptions(final LockItem lockItem, final boolean deleteLock, final boolean bestEffort, final Optional<ByteBuffer> data, Map<String, AttributeValueUpdate> additionalAttributeUpdates) {
        this.lockItem = lockItem;
        this.deleteLock = deleteLock;
        this.bestEffort = bestEffort;
        this.data = data;
        this.additionalAttributeUpdates = additionalAttributeUpdates;
    }

    public static class ReleaseLockOptionsBuilder {
        private final LockItem lockItem;
        private boolean deleteLock;
        private boolean bestEffort;
        private Optional<ByteBuffer> data;
        private Map<String, AttributeValueUpdate> additionalAttributeUpdates;

        ReleaseLockOptionsBuilder(final LockItem lockItem) {
            this.lockItem = lockItem;
            this.deleteLock = true;
            this.bestEffort = false;
            this.data = Optional.empty();
            this.additionalAttributeUpdates = new HashMap<>();
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

        /**
         * Stores some additional attributes with the lock. This can be used to add/update any arbitrary parameters to
         * the lock row.
         *
         * @param additionalAttributeUpdates an arbitrary map of attribute updates to store with the lock row to be acquired
         * @return a reference to this builder for fluent method chaining
         */
        public ReleaseLockOptionsBuilder withAdditionalAttributeUpdates(Map<String, AttributeValueUpdate> additionalAttributeUpdates) {
            this.additionalAttributeUpdates = additionalAttributeUpdates;
            return this;
        }

        public ReleaseLockOptions build() {
            return new ReleaseLockOptions(this.lockItem, this.deleteLock, this.bestEffort, this.data, this.additionalAttributeUpdates);
        }

        @Override
        public String toString() {
            return String.format("ReleaseLockOptions.ReleaseLockOptionsBuilder(lockItem=%s, deleteLock=%s, bestEffort=%s, data=%s, additionalAttributeUpdates=%s)", lockItem, deleteLock, bestEffort, data, additionalAttributeUpdates);
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

    Map<String, AttributeValueUpdate> getAdditionalAttributeUpdates() {
        return additionalAttributeUpdates;
    }
}
