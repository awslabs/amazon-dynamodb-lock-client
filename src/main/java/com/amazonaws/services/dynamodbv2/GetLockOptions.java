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

/**
 * Provides options for getting a lock when calling the getLock() method.
 */
public class GetLockOptions {
    private final String partitionKey;
    private final Optional<String> sortKey;
    private final boolean deleteLockOnRelease;


    public static class GetLockOptionsBuilder {
        private String partitionKey;
        private Optional<String> sortKey;
        private boolean deleteLockOnRelease;

        GetLockOptionsBuilder(final String partitionKey) {
            this.sortKey = Optional.empty();
            this.deleteLockOnRelease = false;
            this.partitionKey = partitionKey;
        }

        public GetLockOptionsBuilder withSortKey(final String sortKey) {
            this.sortKey = Optional.ofNullable(sortKey);
            return this;
        }

        public GetLockOptionsBuilder withDeleteLockOnRelease(final boolean deleteLockOnRelease) {
            this.deleteLockOnRelease = deleteLockOnRelease;
            return this;
        }

        public GetLockOptions build() {
            return new GetLockOptions(this.partitionKey, this.sortKey, this.deleteLockOnRelease);
        }

        @Override
        public java.lang.String toString() {
            return "GetLockOptions.GetLockOptionsBuilder(partitionKey=" + this.partitionKey + ", sortKey=" + this.sortKey + ", deleteLockOnRelease=" + this.deleteLockOnRelease + ")";
        }
    }

    /**
     * Creates a GetLockOptionsBuilder, which lets the caller specify arguments
     * to getLock(). The only required parameter is the partition key, though the
     * sort key is required if the table has a sort key.
     *
     * @param partitionKey The partitionKey for the lock.
     * @return The GetLockOptionsBuilder, which can be used to specify other
     * optional arguments.
     */
    public static GetLockOptionsBuilder builder(final String partitionKey) {
        return new GetLockOptionsBuilder(partitionKey);
    }

    private GetLockOptions(final String key, final Optional<String> sortKey, final boolean deleteLockOnRelease) {
        this.partitionKey = key;
        this.sortKey = sortKey;
        this.deleteLockOnRelease = deleteLockOnRelease;
    }

    String getPartitionKey() {
        return this.partitionKey;
    }

    Optional<String> getSortKey() {
        return this.sortKey;
    }

    boolean isDeleteLockOnRelease() {
        return this.deleteLockOnRelease;
    }
}