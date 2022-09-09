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

import java.util.Optional;

import software.amazon.awssdk.metrics.MetricCollector;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;

/**
 * An options class for the createDynamoDBTable method in the lock client. This
 * allows the user to create a DynamoDB table that is lock client-compatible and
 * specify optional parameters such as the desired throughput and whether or not
 * to use a sort key.
 *
 * @author slutsker
 */
public class CreateDynamoDBTableOptions {
    private final DynamoDbClient dynamoDBClient;
    private final ProvisionedThroughput provisionedThroughput;
    private final String tableName;
    private final String partitionKeyName;
    private final Optional<String> sortKeyName;
    private final Optional<MetricCollector> metricCollector;

    CreateDynamoDBTableOptions(final DynamoDbClient dynamoDBClient, final ProvisionedThroughput provisionedThroughput, final String tableName, final String partitionKeyName,
        final Optional<String> sortKeyName, final Optional<MetricCollector> metricCollector) {
        this.dynamoDBClient = dynamoDBClient;
        this.provisionedThroughput = provisionedThroughput;
        this.tableName = tableName;
        this.partitionKeyName = partitionKeyName;
        this.sortKeyName = sortKeyName;
        this.metricCollector = metricCollector;
    }

    public static class CreateDynamoDBTableOptionsBuilder {
        private DynamoDbClient dynamoDBClient;
        private ProvisionedThroughput provisionedThroughput;
        private String tableName;
        private String partitionKeyName;
        private Optional<String> sortKeyName;
        private Optional<MetricCollector> metricCollector;

        CreateDynamoDBTableOptionsBuilder(final DynamoDbClient dynamoDBClient, final ProvisionedThroughput provisionedThroughput, final String tableName) {
            this.dynamoDBClient = dynamoDBClient;
            this.provisionedThroughput = provisionedThroughput;
            this.tableName = tableName;
            this.partitionKeyName = AmazonDynamoDBLockClientOptions.DEFAULT_PARTITION_KEY_NAME;
            this.sortKeyName = Optional.empty();
            this.metricCollector = Optional.empty();
        }

        /**
         * @param partitionKeyName The partition key name of the table. If not specified, the default "key" will be used.
         * @return this
         */
        public CreateDynamoDBTableOptionsBuilder withPartitionKeyName(final String partitionKeyName) {
            this.partitionKeyName = partitionKeyName;
            return this;
        }

        /**
         * @param sortKeyName The sort key name of the table. If not specified, the table will only have a partition key.
         * @return this
         */
        public CreateDynamoDBTableOptionsBuilder withSortKeyName(final String sortKeyName) {
            this.sortKeyName = Optional.ofNullable(sortKeyName);
            return this;
        }

        /**
         * @param metricCollector metrics collector, optionally.
         * @return this
         */
        public CreateDynamoDBTableOptionsBuilder withMetricCollector(final MetricCollector metricCollector) {
            this.metricCollector = Optional.ofNullable(metricCollector);
            return this;
        }

        public CreateDynamoDBTableOptions build() {
            return new CreateDynamoDBTableOptions(this.dynamoDBClient, this.provisionedThroughput, this.tableName, this.partitionKeyName, this.sortKeyName,
                metricCollector);
        }

        @Override
        public java.lang.String toString() {
            return "CreateDynamoDBTableOptions.CreateDynamoDBTableOptionsBuilder(dynamoDBClient=" + this.dynamoDBClient + ", provisionedThroughput=" + this.provisionedThroughput
                + ", tableName=" + this.tableName + ", partitionKeyName=" + this.partitionKeyName + ", sortKeyName=" + this.sortKeyName + ", metricCollector=" + this.metricCollector + ")";
        }
    }

    /**
     * Creates a builder for a CreateDynamoDBTableOptions object. The three
     * required parameters are the DynamoDB client, the provisioned throughout,
     * and the table name. The rest have defaults and can be configured after
     * the fact.
     *
     * @param dynamoDBClient        The DynamoDB Client under which to make the table.
     * @param provisionedThroughput The throughout to allocate to the table.
     * @param tableName             The table name to create.
     * @return a builder for CreateDynamoDBTableOptions instances
     */
    public static CreateDynamoDBTableOptionsBuilder builder(final DynamoDbClient dynamoDBClient, final ProvisionedThroughput provisionedThroughput, final String tableName) {
        return new CreateDynamoDBTableOptionsBuilder(dynamoDBClient, provisionedThroughput, tableName);
    }

    DynamoDbClient getDynamoDBClient() {
        return this.dynamoDBClient;
    }

    ProvisionedThroughput getProvisionedThroughput() {
        return this.provisionedThroughput;
    }

    String getTableName() {
        return this.tableName;
    }

    String getPartitionKeyName() {
        return this.partitionKeyName;
    }

    Optional<String> getSortKeyName() {
        return this.sortKeyName;
    }

    Optional<MetricCollector> getMetricCollector() {
        return metricCollector;
    }
}