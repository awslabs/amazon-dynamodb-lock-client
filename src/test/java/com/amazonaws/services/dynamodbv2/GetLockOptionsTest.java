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
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.amazonaws.metrics.RequestMetricCollector;

/**
 * Tests the features of the lock client that involve getting all the locks
 * from DynamoDB.
 *
 * @author <a href="mailto:amcp@amazon.co.jp">Alexander Patrikalakis</a> 2017-07-13
 */
@RunWith(MockitoJUnitRunner.class)
public class GetLockOptionsTest {

    public static final String KEY_0 = "key0";
    public static final String RANGE_KEY_0 = "rangeKey0";
    @Mock
    RequestMetricCollector metricCollector;

    @Test
    public void test_expectedInstanceProduced_whenChainingMethods() {
        GetLockOptions.GetLockOptionsBuilder builder = new GetLockOptions.GetLockOptionsBuilder(KEY_0)
            .withDeleteLockOnRelease(true)
            .withSortKey(RANGE_KEY_0)
            .withRequestMetricCollector(this.metricCollector);
        System.out.println(builder.toString());

        final GetLockOptions options = builder.build();

        assertEquals(KEY_0, options.getPartitionKey());
        assertTrue(options.isDeleteLockOnRelease());
        assertEquals(RANGE_KEY_0, options.getSortKey().get());
        assertEquals(this.metricCollector, options.getRequestMetricCollector().get());

    }

}
