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

/**
 * Usage example listed in README.md. Start DynamoDB Local first on port 4567.
 * Start DynamoDB Local on port 4567 for this example (will happen automatically
 * when running mvn clean install verify -Pintegration-tests).
 *
 * @author <a href="mailto:amcp@amazon.co.jp">Alexander Patrikalakis</a> 2017-05-05
 */
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;

public class LockClientExample {
    private static final AwsClientBuilder.EndpointConfiguration DYNAMODB_LOCAL_ENDPOINT =
            new AwsClientBuilder.EndpointConfiguration("http://localhost:4567",
                    "us-west-2");
    @Test
    public void usageExample() throws InterruptedException, IOException {
        // Inject client configuration to the builder like the endpoint and signing region
        final AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(DYNAMODB_LOCAL_ENDPOINT)
                .withCredentials(new DefaultAWSCredentialsProviderChain())
                .build();
        // Whether or not to create a heartbeating background thread
        final boolean createHeartbeatBackgroundThread = true;
        //build the lock client
        final AmazonDynamoDBLockClient client = new AmazonDynamoDBLockClient(
            AmazonDynamoDBLockClientOptions.builder(dynamoDB, "lockTable")
                    .withTimeUnit(TimeUnit.SECONDS)
                    .withLeaseDuration(10L)
                    .withHeartbeatPeriod(3L)
                    .withCreateHeartbeatBackgroundThread(createHeartbeatBackgroundThread)
                    .build());
        //try to acquire a lock on the partition key "Moe"
        final Optional<LockItem> lockItem =
                client.tryAcquireLock(AcquireLockOptions.builder("Moe").build());
        if (lockItem.isPresent()) {
            System.out.println("Acquired lock! If I die, my lock will expire in 10 seconds.");
            System.out.println("Otherwise, I will hold it until I stop heartbeating.");
            client.releaseLock(lockItem.get());
        } else {
            System.out.println("Failed to acquire lock!");
        }
        client.close();
    }
}
