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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;

import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;

/**
 * Unit tests for CreateDynamoDBTableOptions.
 *
 * @author <a href="mailto:amcp@amazon.com">Alexander Patrikalakis</a> 2017-07-13
 */
@RunWith(MockitoJUnitRunner.class)
public class CreateDynamoDBTableOptionsTest {
    @Mock
    DynamoDbClient dynamodb;
    @Test
    public void builder_whenDynamoDbClientReset_isSame() {
        CreateDynamoDBTableOptions.CreateDynamoDBTableOptionsBuilder builder =
            CreateDynamoDBTableOptions.builder(dynamodb, ProvisionedThroughput.builder().readCapacityUnits(1L).writeCapacityUnits(1L).build(),"table");
        assertTrue(dynamodb == builder.build().getDynamoDBClient());
    }
    @Test
    public void builder_whenProvisionedThroughputReset_isSame() {
        ProvisionedThroughput pt = ProvisionedThroughput.builder().readCapacityUnits(1L).writeCapacityUnits(1L).build();
        CreateDynamoDBTableOptions.CreateDynamoDBTableOptionsBuilder builder =
            CreateDynamoDBTableOptions.builder(dynamodb, pt,"table");
        assertTrue(pt == builder.build().getProvisionedThroughput());
    }

    @Test
    public void builder_whenTableNameReset_isSame() {
        String tableName = "table";
        CreateDynamoDBTableOptions.CreateDynamoDBTableOptionsBuilder builder =
            CreateDynamoDBTableOptions.builder(dynamodb, ProvisionedThroughput.builder().readCapacityUnits(1L).writeCapacityUnits(1L).build(), tableName);
        assertTrue(tableName == builder.build().getTableName());
    }

    @Test
    public void builder_whenPartitionKeyNameReset_isSame() {
        CreateDynamoDBTableOptions.CreateDynamoDBTableOptionsBuilder builder =
            CreateDynamoDBTableOptions.builder(dynamodb, ProvisionedThroughput.builder().readCapacityUnits(1L).writeCapacityUnits(1L).build(),"table");
        builder.withPartitionKeyName(null);
        assertNull(builder.build().getPartitionKeyName());
        String partitionKeyName = "key";
        builder.withPartitionKeyName(partitionKeyName);
        assertEquals(partitionKeyName, builder.build().getPartitionKeyName());
    }
}
