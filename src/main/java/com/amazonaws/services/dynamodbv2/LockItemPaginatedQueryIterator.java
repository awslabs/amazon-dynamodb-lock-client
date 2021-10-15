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

import java.util.Objects;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

import static java.util.stream.Collectors.toList;

/**
 * Lazy-loaded. Not immutable. Not thread safe.
 */
final class LockItemPaginatedQueryIterator extends LockItemPaginatedIterator {
  private final DynamoDbClient dynamoDB;
  private volatile QueryRequest queryRequest;
  private final LockItemFactory lockItemFactory;

  /**
   * Initially null to indicate that no pages have been loaded yet.
   * Afterwards, its {@link QueryResponse#lastEvaluatedKey()} is used to tell
   * if there are more pages to load if its
   * {@link QueryResponse#lastEvaluatedKey()} is not null.
   */
  private volatile QueryResponse queryResponse = null;

  LockItemPaginatedQueryIterator(final DynamoDbClient dynamoDB, final QueryRequest queryRequest, final LockItemFactory lockItemFactory) {
    this.dynamoDB = Objects.requireNonNull(dynamoDB, "dynamoDB must not be null");
    this.queryRequest = Objects.requireNonNull(queryRequest, "queryRequest must not be null");
    this.lockItemFactory = Objects.requireNonNull(lockItemFactory, "lockItemFactory must not be null");
  }

  protected boolean hasAnotherPageToLoad() {
    if (!this.hasLoadedFirstPage()) {
      return true;
    }

    return this.queryResponse.lastEvaluatedKey() != null && !this.queryResponse.lastEvaluatedKey().isEmpty();
  }

  protected boolean hasLoadedFirstPage() {
    return this.queryResponse != null;
  }

  protected void loadNextPageIntoResults() {
    this.queryResponse = this.dynamoDB.query(this.queryRequest);

    this.currentPageResults = this.queryResponse.items().stream().map(this.lockItemFactory::create).collect(toList());
    this.currentPageResultsIndex = 0;

    this.queryRequest = QueryRequest.builder()
        .tableName(queryRequest.tableName())
        .keyConditionExpression(queryRequest.keyConditionExpression())
        .expressionAttributeNames(queryRequest.expressionAttributeNames())
        .expressionAttributeValues(queryRequest.expressionAttributeValues())
        .exclusiveStartKey(queryResponse.lastEvaluatedKey())
        .build();
  }
}
