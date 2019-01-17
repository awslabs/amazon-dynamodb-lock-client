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

import static java.util.stream.Collectors.toList;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

/**
 * Lazy-loaded. Not immutable. Not thread safe.
 */
final class LockItemPaginatedScanIterator implements Iterator<LockItem> {
    private final DynamoDbClient dynamoDB;
    private volatile ScanRequest scanRequest;
    private final LockItemFactory lockItemFactory;

    private List<LockItem> currentPageResults = Collections.emptyList();
    private int currentPageResultsIndex = 0;

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

    @Override
    public boolean hasNext() {
        while (this.currentPageResultsIndex == this.currentPageResults.size() && this.hasAnotherPageToLoad()) {
            this.loadNextPageIntoResults();
        }

        return this.currentPageResultsIndex < this.currentPageResults.size();
    }

    @Override
    public LockItem next() throws NoSuchElementException {
        if (!this.hasNext()) {
            throw new NoSuchElementException();
        }

        final LockItem next = this.currentPageResults.get(this.currentPageResultsIndex);
        this.currentPageResultsIndex++;

        return next;
    }

    @Override
    public void remove() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("This iterator is immutable.");
    }

    private boolean hasAnotherPageToLoad() {
        if (!this.hasLoadedFirstPage()) {
            return true;
        }

        return this.scanResponse.lastEvaluatedKey() != null && !this.scanResponse.lastEvaluatedKey().isEmpty();
    }

    private boolean hasLoadedFirstPage() {
        return this.scanResponse != null;
    }

    private void loadNextPageIntoResults() {
        this.scanResponse = this.dynamoDB.scan(this.scanRequest);

        this.currentPageResults = this.scanResponse.items().stream().map(this.lockItemFactory::create).collect(toList());
        this.currentPageResultsIndex = 0;

        this.scanRequest = ScanRequest.builder()
                .tableName(scanRequest.tableName())
                .exclusiveStartKey(scanResponse.lastEvaluatedKey())
                .build();

    }
}
