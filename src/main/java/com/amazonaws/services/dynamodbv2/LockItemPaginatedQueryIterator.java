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
