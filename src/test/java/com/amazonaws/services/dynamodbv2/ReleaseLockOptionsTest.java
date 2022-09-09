/**
 * Copyright 2013-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.metrics.MetricCollector;
import software.amazon.awssdk.metrics.NoOpMetricCollector;

/**
 * ReleaseLockOptions unit tests.
 *
 * @author <a href="mailto:amcp@amazon.com">Alexander Patrikalakis</a> 2017-07-13
 */
@RunWith(MockitoJUnitRunner.class)
public class ReleaseLockOptionsTest {
    @Mock
    AmazonDynamoDBLockClient lockClient;

    MetricCollector metricCollector = NoOpMetricCollector.create();
    @Test
    public void withLockItem_setsLockItem() {
        LockItem lockItem = LockItemTest.createLockItem(lockClient);
        ReleaseLockOptions.ReleaseLockOptionsBuilder builder = ReleaseLockOptions.builder(lockItem).withMetricCollector(metricCollector);
        System.out.println(builder.toString());
        assertEquals(lockItem, builder.build().getLockItem());
        assertEquals(builder.build().getMetricCollector().get(), metricCollector);
    }
}
