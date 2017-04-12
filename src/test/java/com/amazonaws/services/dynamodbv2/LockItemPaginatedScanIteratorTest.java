package com.amazonaws.services.dynamodbv2;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.internal.InternalUtils;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

/**
 * Unit tests for LockItemPaginatedScanIterator.
 *
 * @author <a href="mailto:amcp@amazon.co.jp">Alexander Patrikalakis</a> 2017-07-13
 */
@RunWith(MockitoJUnitRunner.class)
public class LockItemPaginatedScanIteratorTest {
    @Mock
    AmazonDynamoDB dynamodb;
    @Mock
    LockItemFactory factory;

    @Test(expected = UnsupportedOperationException.class)
    public void remove_throwsUnsupportedOperationException() {
        LockItemPaginatedScanIterator sut = new LockItemPaginatedScanIterator(dynamodb, new ScanRequest(), factory);
        sut.remove();
    }

    @Test(expected = NoSuchElementException.class)
    public void next_whenDoesNotHaveNext_throwsNoSuchElementException() {
        ScanRequest request = new ScanRequest();
        LockItemPaginatedScanIterator sut = new LockItemPaginatedScanIterator(dynamodb, request, factory);
        List<Map<String, AttributeValue>> list1 = new ArrayList<>();
        list1.add(InternalUtils.toAttributeValues(new Item()));
        when(dynamodb.scan(any()))
            .thenReturn(new ScanResult().withItems(list1).withCount(1).withLastEvaluatedKey(new HashMap<>()))
            .thenReturn(new ScanResult().withItems(Collections.emptyList()).withCount(0));
        sut.next();
        sut.next();
    }
}
