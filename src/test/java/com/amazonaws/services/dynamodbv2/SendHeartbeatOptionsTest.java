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

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.amazonaws.metrics.RequestMetricCollector;

/**
 * SendHeartbeatOptions unit tests.
 *
 * @author <a href="mailto:amcp@amazon.co.jp">Alexander Patrikalakis</a> 2017-07-13
 */
@RunWith(MockitoJUnitRunner.class)
public class SendHeartbeatOptionsTest {
    @Mock
    RequestMetricCollector metricCollector;
    @Mock
    AmazonDynamoDBLockClient lockClient;

    @Test
    public void test_initializeOptionsWithRequestMetrics() {
        final SendHeartbeatOptions options0 = SendHeartbeatOptions.builder(null).withRequestMetricCollector(this.metricCollector).build();

        final SendHeartbeatOptions options1 = SendHeartbeatOptions.builder(null).withRequestMetricCollector(this.metricCollector).build();

        LockItem lockItem = LockItemTest.createLockItem(lockClient);
        final SendHeartbeatOptions.SendHeartbeatOptionsBuilder builder =
            SendHeartbeatOptions.builder(lockItem)
            .withRequestMetricCollector(this.metricCollector);
        System.out.println(builder.toString());
        final SendHeartbeatOptions options2 = builder.build();

        assertEquals(this.metricCollector, options0.getRequestMetricCollector().get());
        assertEquals(this.metricCollector, options1.getRequestMetricCollector().get());
        assertEquals(this.metricCollector, options2.getRequestMetricCollector().get());
    }
}
