package com.amazonaws.services.dynamodbv2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import junit.framework.TestCase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class LockItemPaginatedQueryIteratorTest extends TestCase {
  @Mock
  DynamoDbClient dynamodb;
  @Mock
  LockItemFactory factory;

  @Test(expected = UnsupportedOperationException.class)
  public void remove_throwsUnsupportedOperationException() {
    LockItemPaginatedQueryIterator
        sut = new LockItemPaginatedQueryIterator(dynamodb, QueryRequest.builder().build(), factory);
    sut.remove();
  }

  @Test(expected = NoSuchElementException.class)
  public void next_whenDoesNotHaveNext_throwsNoSuchElementException() {
    QueryRequest request = QueryRequest.builder().build();
    LockItemPaginatedQueryIterator
        sut = new LockItemPaginatedQueryIterator(dynamodb, request, factory);
    List<Map<String, AttributeValue>> list1 = new ArrayList<>();
    list1.add(new HashMap<>());
    when(dynamodb.query(ArgumentMatchers.<QueryRequest>any()))
        .thenReturn(
            QueryResponse.builder().items(list1).count(1).lastEvaluatedKey(new HashMap<>()).build())
        .thenReturn(QueryResponse.builder().items(Collections.emptyList()).count(0).build());
    sut.next();
    sut.next();
  }
}
