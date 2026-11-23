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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;

import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

/**
 * Unit tests for LockItemPaginatedScanIterator.
 *
 * @author <a href="mailto:amcp@amazon.com">Alexander Patrikalakis</a> 2017-07-13
 */
@RunWith(MockitoJUnitRunner.class)
public class LockItemPaginatedScanIteratorTest {
    @Mock
    DynamoDbClient dynamodb;
    @Mock
    LockItemFactory factory;

    @Test(expected = UnsupportedOperationException.class)
    public void remove_throwsUnsupportedOperationException() {
        LockItemPaginatedScanIterator sut = new LockItemPaginatedScanIterator(dynamodb, ScanRequest.builder().build(), factory);
        sut.remove();
    }

    @Test(expected = NoSuchElementException.class)
    public void next_whenDoesNotHaveNext_throwsNoSuchElementException() {
        ScanRequest request = ScanRequest.builder().build();
        LockItemPaginatedScanIterator sut = new LockItemPaginatedScanIterator(dynamodb, request, factory);
        List<Map<String, AttributeValue>> list1 = new ArrayList<>();
        list1.add(new HashMap<>());
        when(dynamodb.scan(ArgumentMatchers.<ScanRequest>any()))
            .thenReturn(ScanResponse.builder().items(list1).count(1).lastEvaluatedKey(new HashMap<>()).build())
            .thenReturn(ScanResponse.builder().items(Collections.emptyList()).count(0).build());
        sut.next();
        sut.next();
    }

    @Test
    public void next_whenMultiplePages_returnsAllItemsAndStopsAtTerminalPage() {
        LockItemPaginatedScanIterator sut = new LockItemPaginatedScanIterator(dynamodb, ScanRequest.builder().build(), factory);
        Map<String, AttributeValue> pageItem = new HashMap<>();
        Map<String, AttributeValue> lastEvaluatedKey = new HashMap<>();
        lastEvaluatedKey.put("has_next", AttributeValue.builder().s("true").build());
        LockItem first = mock(LockItem.class);
        LockItem second = mock(LockItem.class);

        when(factory.create(ArgumentMatchers.<Map<String, AttributeValue>>any())).thenReturn(first).thenReturn(second);
        when(dynamodb.scan(ArgumentMatchers.<ScanRequest>any()))
            .thenReturn(ScanResponse.builder().items(Collections.singletonList(pageItem)).count(1)
                .lastEvaluatedKey(lastEvaluatedKey).build())
            .thenReturn(ScanResponse.builder().items(Collections.singletonList(pageItem)).count(1).build());

        assertTrue(sut.hasNext());
        assertEquals(first, sut.next());
        assertTrue(sut.hasNext());
        assertEquals(second, sut.next());
        assertFalse(sut.hasNext());
        assertFalse(sut.hasNext());
        verify(dynamodb, times(2)).scan(ArgumentMatchers.<ScanRequest>any());
    }

    @Test
    public void hasNext_whenFirstPageIsEmpty_returnsFalseWithoutReloading() {
        LockItemPaginatedScanIterator sut = new LockItemPaginatedScanIterator(dynamodb, ScanRequest.builder().build(), factory);
        when(dynamodb.scan(ArgumentMatchers.<ScanRequest>any()))
            .thenReturn(ScanResponse.builder().items(Collections.emptyList()).count(0).build());

        assertFalse(sut.hasNext());
        assertFalse(sut.hasNext());
        verify(dynamodb, times(1)).scan(ArgumentMatchers.<ScanRequest>any());
    }
}
