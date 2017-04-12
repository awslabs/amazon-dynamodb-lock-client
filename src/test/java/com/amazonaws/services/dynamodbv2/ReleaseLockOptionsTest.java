package com.amazonaws.services.dynamodbv2;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.amazonaws.metrics.RequestMetricCollector;

/**
 * ReleaseLockOptions unit tests.
 *
 * @author <a href="mailto:amcp@amazon.co.jp">Alexander Patrikalakis</a> 2017-07-13
 */
@RunWith(MockitoJUnitRunner.class)
public class ReleaseLockOptionsTest {
    @Mock
    AmazonDynamoDBLockClient lockClient;
    @Test
    public void withLockItem_setsLockItem() {
        LockItem lockItem = LockItemTest.createLockItem(lockClient);
        ReleaseLockOptions.ReleaseLockOptionsBuilder builder = ReleaseLockOptions.builder(lockItem)
            .withRequestMetricCollector(RequestMetricCollector.NONE);
        System.out.println(builder.toString());
        assertEquals(lockItem, builder.build().getLockItem());
        assertEquals(builder.build().getRequestMetricCollector().get(), RequestMetricCollector.NONE);
    }
}
