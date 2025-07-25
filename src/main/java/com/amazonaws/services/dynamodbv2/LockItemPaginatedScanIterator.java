/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.amazonaws.services.dynamodbv2;

import java.util.Objects;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

import static java.util.stream.Collectors.toList;

/**
 * Lazy-loaded. Not immutable. Not thread safe.
 */
final class LockItemPaginatedScanIterator extends LockItemPaginatedIterator {
    private final DynamoDbClient dynamoDB;
    private volatile ScanRequest scanRequest;
    private final LockItemFactory lockItemFactory;

    /**
     * Initially null to indicate that no pages have been loaded yet.
     * Afterwards, its {@link ScanResponse#lastEvaluatedKey()} is used to tell
     * if there are more pages to load if its
     * {@link ScanResponse#lastEvaluatedKey()} is not null.
     */
    private volatile ScanResponse scanResponse = null;

    LockItemPaginatedScanIterator(final DynamoDbClient dynamoDB, final ScanRequest scanRequest, final LockItemFactory lockItemFactory) {
        this.dynamoDB = Objects.requireNonNull(dynamoDB, "dynamoDB must not be null");
        this.scanRequest = Objects.requireNonNull(scanRequest, "scanRequest must not be null");
        this.lockItemFactory = Objects.requireNonNull(lockItemFactory, "lockItemFactory must not be null");
    }

    protected boolean hasAnotherPageToLoad() {
        if (!this.hasLoadedFirstPage()) {
            return true;
        }

        return this.scanResponse.lastEvaluatedKey() != null && !this.scanResponse.lastEvaluatedKey().isEmpty();
    }

    protected boolean hasLoadedFirstPage() {
        return this.scanResponse != null;
    }

    protected void loadNextPageIntoResults() {
        this.scanResponse = this.dynamoDB.scan(this.scanRequest);

        this.currentPageResults = this.scanResponse.items().stream().map(this.lockItemFactory::create).collect(toList());
        this.currentPageResultsIndex = 0;

        this.scanRequest = ScanRequest.builder()
                .tableName(scanRequest.tableName())
                .exclusiveStartKey(scanResponse.lastEvaluatedKey())
                .build();

    }
}
