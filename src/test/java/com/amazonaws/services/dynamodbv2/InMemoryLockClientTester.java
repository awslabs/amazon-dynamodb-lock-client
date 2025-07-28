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

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.SecureRandom;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;

import static org.mockito.AdditionalAnswers.delegatesTo;

/**
 * Base class for lock client tests. Sets up in-memory version of DynamoDB.
 *
 * @author <a href="mailto:slutsker@amazon.com">Sasha Slutsker</a>
 * @author <a href="mailto:amcp@amazon.com">Alexander Patrikalakis</a>
 */
public abstract class InMemoryLockClientTester {
    protected static final String LOCALHOST = "mars";
    protected static final String INTEGRATION_TESTER = "integrationTester";
    protected static final SecureRandom SECURE_RANDOM;

    static {
        SECURE_RANDOM = new SecureRandom();
    }

    protected static final String TABLE_NAME = "test";
    protected static final String RANGE_KEY_TABLE_NAME = "rangeTest";
    protected static final String TEST_DATA = "test_data";
    protected static final long SHORT_LEASE_DUR = 60L; //60 milliseconds

    protected static final AcquireLockOptions ACQUIRE_LOCK_OPTIONS_TEST_KEY_1 = AcquireLockOptions.builder("testKey1").withData(ByteBuffer.wrap(TEST_DATA.getBytes())).build();
    protected static final AcquireLockOptions ACQUIRE_LOCK_OPTIONS_TEST_KEY_1_SORT_1 = AcquireLockOptions.builder("testKey1").withSortKey("1").withData(ByteBuffer.wrap(TEST_DATA.getBytes())).build();
    protected static final AcquireLockOptions ACQUIRE_LOCK_OPTIONS_TEST_KEY_1_SORT_2 = AcquireLockOptions.builder("testKey1").withSortKey("2").withData(ByteBuffer.wrap(TEST_DATA.getBytes())).build();

    protected static final AcquireLockOptions ACQUIRE_LOCK_OPTIONS_ONLY_IF_LOCK_EXIST_UPDATE_FALSE =
            AcquireLockOptions.builder("testKey1").withData(ByteBuffer.wrap(TEST_DATA.getBytes())).withAcquireOnlyIfLockAlreadyExists
                    (true).withReplaceData(false).withUpdateExistingLockRecord(false).build();

    protected static final AcquireLockOptions ACQUIRE_LOCK_OPTIONS_ONLY_IF_LOCK_EXIST_UPDATE_FALSE_CONSISTENT_DATA_TRUE =
            AcquireLockOptions.builder("testKey1").withData(ByteBuffer.wrap(TEST_DATA.getBytes())).withAcquireOnlyIfLockAlreadyExists
                    (true).withReplaceData(false).withUpdateExistingLockRecord(false).withAcquireReleasedLocksConsistently(true).build();

    protected static final AcquireLockOptions ACQUIRE_LOCK_OPTIONS_ONLY_IF_LOCK_EXIST_UPDATE_FALSE_SORT_1 =
            AcquireLockOptions.builder("testKey1").withSortKey("1").withData(ByteBuffer.wrap(TEST_DATA.getBytes()))
                    .withAcquireOnlyIfLockAlreadyExists(true).withReplaceData(false).withUpdateExistingLockRecord(false).build();

    protected static final AcquireLockOptions ACQUIRE_LOCK_OPTIONS_ONLY_IF_LOCK_EXIST_UPDATE_FALSE_CONSISTENT_DATA_TRUE_SORT_1 =
            AcquireLockOptions.builder("testKey1").withSortKey("1").withData(ByteBuffer.wrap(TEST_DATA.getBytes()))
                    .withAcquireOnlyIfLockAlreadyExists(true).withReplaceData(false).withUpdateExistingLockRecord(false)
                    .withAcquireReleasedLocksConsistently(true).build();

    protected static final AcquireLockOptions ACQUIRE_LOCK_OPTIONS_ONLY_IF_LOCK_EXIST_UPDATE_TRUE =
            AcquireLockOptions.builder("testKey1").withData(ByteBuffer.wrap(TEST_DATA.getBytes())).withAcquireOnlyIfLockAlreadyExists(true)
                    .withReplaceData(false).withUpdateExistingLockRecord(true).build();

    protected static final AcquireLockOptions ACQUIRE_LOCK_OPTIONS_ONLY_IF_LOCK_EXIST_UPDATE_TRUE_CONSISTENT_DATA_TRUE =
            AcquireLockOptions.builder("testKey1").withData(ByteBuffer.wrap(TEST_DATA.getBytes())).withAcquireOnlyIfLockAlreadyExists(true)
                    .withReplaceData(false).withUpdateExistingLockRecord(true).withAcquireReleasedLocksConsistently(true).build();

    protected static final AcquireLockOptions ACQUIRE_LOCK_OPTIONS_ONLY_IF_LOCK_EXIST_UPDATE_TRUE_SORT_1 =
            AcquireLockOptions.builder("testKey1").withSortKey("1").withData(ByteBuffer.wrap(TEST_DATA.getBytes()))
                    .withAcquireOnlyIfLockAlreadyExists(true).withReplaceData(false).withUpdateExistingLockRecord(true).build();

    protected static final AcquireLockOptions ACQUIRE_LOCK_OPTIONS_ONLY_IF_LOCK_EXIST_UPDATE_TRUE_CONSISTENT_DATA_TRUE_SORT_1 =
            AcquireLockOptions.builder("testKey1").withSortKey("1").withData(ByteBuffer.wrap(TEST_DATA.getBytes())).withAcquireOnlyIfLockAlreadyExists(true)
                    .withReplaceData(false).withUpdateExistingLockRecord(true).withAcquireReleasedLocksConsistently(true).build();

    protected static final AcquireLockOptions ACQUIRE_LOCK_OPTIONS_REPLACE_DATA_FALSE =
        AcquireLockOptions.builder("testKey1").withData(ByteBuffer.wrap(TEST_DATA.getBytes())).withReplaceData(false).build();

    protected static final AcquireLockOptions ACQUIRE_LOCK_OPTIONS_REPLACE_DATA_FALSE_SORT_1 =
            AcquireLockOptions.builder("testKey1").withSortKey("1").withData(ByteBuffer.wrap(TEST_DATA.getBytes())).withReplaceData(false).build();

    protected static final AcquireLockOptions ACQUIRE_LOCK_OPTIONS_REPLACE_DATA_FALSE_CONSISTENT_DATA_TRUE =
            AcquireLockOptions.builder("testKey1").withData(ByteBuffer.wrap(TEST_DATA.getBytes())).withReplaceData(false)
                    .withAcquireReleasedLocksConsistently(true).build();

    protected static final AcquireLockOptions ACQUIRE_LOCK_OPTIONS_REPLACE_DATA_FALSE_CONSISTENT_DATA_TRUE_SORT_1 =
            AcquireLockOptions.builder("testKey1").withSortKey("1").withData(ByteBuffer.wrap(TEST_DATA.getBytes())).withReplaceData(false)
                    .withAcquireReleasedLocksConsistently(true).build();

    protected static final AcquireLockOptions ACQUIRE_LOCK_OPTIONS_REPLACE_DATA_FALSE_UPDATE_EXISTING_TRUE =
            AcquireLockOptions.builder("testKey1").withData(ByteBuffer.wrap(TEST_DATA.getBytes())).withReplaceData(false).withUpdateExistingLockRecord(true).build();

    protected static final AcquireLockOptions ACQUIRE_LOCK_OPTIONS_REPLACE_DATA_FALSE_UPDATE_EXISTING_TRUE_SORT_1 =
            AcquireLockOptions.builder("testKey1").withSortKey("1").withData(ByteBuffer.wrap(TEST_DATA.getBytes())).withReplaceData(false).withUpdateExistingLockRecord(true).build();

    protected static final AcquireLockOptions ACQUIRE_LOCK_OPTIONS_REPLACE_DATA_FALSE_UPDATE_EXISTING_TRUE_CONSISTENT_DATA_TRUE =
            AcquireLockOptions.builder("testKey1").withData(ByteBuffer.wrap(TEST_DATA.getBytes())).withReplaceData(false).withUpdateExistingLockRecord(true)
                    .withAcquireReleasedLocksConsistently(true).build();

    protected static final AcquireLockOptions ACQUIRE_LOCK_OPTIONS_REPLACE_DATA_FALSE_UPDATE_EXISTING_TRUE_CONSISTENT_DATA_TRUE_SORT_1 =
            AcquireLockOptions.builder("testKey1").withSortKey("1").withData(ByteBuffer.wrap(TEST_DATA.getBytes())).withReplaceData(false).withUpdateExistingLockRecord(true).withAcquireReleasedLocksConsistently(true).build();

    protected static final AcquireLockOptions ACQUIRE_LOCK_OPTIONS_TEST_KEY_2 = AcquireLockOptions.builder("testKey2").withData(ByteBuffer.wrap(TEST_DATA.getBytes())).build();

    protected static final AcquireLockOptions ACQUIRE_LOCK_OPTIONS_WITH_NO_WAIT =
        AcquireLockOptions.builder("testKey1").withData(ByteBuffer.wrap(TEST_DATA.getBytes())).withAdditionalTimeToWaitForLock(0L).withRefreshPeriod(0L)
            .withTimeUnit(TimeUnit.SECONDS).build();

    protected static final AcquireLockOptions ACQUIRE_LOCK_OPTIONS_WITH_NO_WAIT_SORT_1 =
            AcquireLockOptions.builder("testKey1").withSortKey("1").withData(ByteBuffer.wrap(TEST_DATA.getBytes()))
                    .withAdditionalTimeToWaitForLock(0L).withRefreshPeriod(0L).withTimeUnit(TimeUnit.SECONDS).build();

    protected static final AcquireLockOptions ACQUIRE_LOCK_OPTIONS_5_SECONDS =
        AcquireLockOptions.builder("testKey1").withData(ByteBuffer.wrap(TEST_DATA.getBytes())).withAdditionalTimeToWaitForLock(5L).withRefreshPeriod(1L)
            .withTimeUnit(TimeUnit.SECONDS).build();

    protected static final GetLockOptions GET_LOCK_OPTIONS_DELETE_ON_RELEASE = GetLockOptions.builder("testKey1").withDeleteLockOnRelease(true).build();

    protected static final GetLockOptions GET_LOCK_OPTIONS_DO_NOT_DELETE_ON_RELEASE = GetLockOptions.builder("testKey1").withDeleteLockOnRelease(false).build();

    protected DynamoDbClient dynamoDBMock;
    protected AmazonDynamoDBLockClient lockClient;
    protected AmazonDynamoDBLockClient lockClientWithHeartbeating;
    protected AmazonDynamoDBLockClient lockClientForRangeKeyTable;
    protected AmazonDynamoDBLockClient lockClientWithHeartbeatingForRangeKeyTable;
    protected AmazonDynamoDBLockClient shortLeaseLockClient;
    protected AmazonDynamoDBLockClient shortLeaseLockClientWithHeartbeating;
    protected AmazonDynamoDBLockClient shortLeaseLockClientForRangeKeyTable;
    protected AmazonDynamoDBLockClient lockClientNoTable;
    protected AmazonDynamoDBLockClientOptions lockClient1Options;

    @Before
    public void setUp() {
        final AwsCredentials credentials = AwsBasicCredentials.create(TABLE_NAME, "d");
        final String endpoint = System.getProperty("dynamodb-local.endpoint");
        if (endpoint == null) {
            throw new IllegalStateException("The JVM was not launched with the dynamodb-local.endpoint system property set");
        }
        if (endpoint.isEmpty()) {
            throw new IllegalStateException("The JVM was not launched with the dynamodb-local.endpoint system property set to a non-empty string");
        }

        this.dynamoDBMock = safelySpyDDB(DynamoDbClient.builder().credentialsProvider(StaticCredentialsProvider.create(credentials))
                .endpointOverride(URI.create(endpoint))
                .region(Region.US_WEST_2)
                .build());

        AmazonDynamoDBLockClient.createLockTableInDynamoDB(CreateDynamoDBTableOptions.builder(this.dynamoDBMock,
                ProvisionedThroughput.builder().readCapacityUnits(10L).writeCapacityUnits(10L).build(),
                TABLE_NAME).build());

        AmazonDynamoDBLockClient.createLockTableInDynamoDB(CreateDynamoDBTableOptions.builder(this.dynamoDBMock,
                ProvisionedThroughput.builder().readCapacityUnits(10L).writeCapacityUnits(10L).build(),
                RANGE_KEY_TABLE_NAME)
                .withSortKeyName("rangeKey").build());

        this.lockClient1Options =
            new AmazonDynamoDBLockClientOptions.AmazonDynamoDBLockClientOptionsBuilder(this.dynamoDBMock, TABLE_NAME, INTEGRATION_TESTER)
                    .withLeaseDuration(3L)
                    .withHeartbeatPeriod(1L)
                    .withTimeUnit(TimeUnit.SECONDS)
                    .withCreateHeartbeatBackgroundThread(false)
                    .build();
        this.lockClient = Mockito.spy(new AmazonDynamoDBLockClient(this.lockClient1Options));
        this.lockClientWithHeartbeating = Mockito.spy(new AmazonDynamoDBLockClient(
            new AmazonDynamoDBLockClientOptions.AmazonDynamoDBLockClientOptionsBuilder(this.dynamoDBMock, TABLE_NAME,
                    INTEGRATION_TESTER).withLeaseDuration(3L).withHeartbeatPeriod(1L)
                .withTimeUnit(TimeUnit.SECONDS).withCreateHeartbeatBackgroundThread(true).build()));
        this.lockClientNoTable = Mockito.spy(new AmazonDynamoDBLockClient(
            new AmazonDynamoDBLockClientOptions.AmazonDynamoDBLockClientOptionsBuilder(this.dynamoDBMock, "doesNotExist", INTEGRATION_TESTER).withLeaseDuration(3L).withHeartbeatPeriod(1L)
                .withTimeUnit(TimeUnit.SECONDS).withCreateHeartbeatBackgroundThread(false).build()));
        this.shortLeaseLockClient = Mockito.spy(new AmazonDynamoDBLockClient(
            new AmazonDynamoDBLockClientOptions.AmazonDynamoDBLockClientOptionsBuilder(this.dynamoDBMock, TABLE_NAME, INTEGRATION_TESTER).withLeaseDuration(SHORT_LEASE_DUR)
                .withHeartbeatPeriod(10L).withTimeUnit(TimeUnit.MILLISECONDS).withCreateHeartbeatBackgroundThread(false).build()));
        this.shortLeaseLockClientWithHeartbeating = Mockito.spy(new AmazonDynamoDBLockClient(
                new AmazonDynamoDBLockClientOptions.AmazonDynamoDBLockClientOptionsBuilder(this.dynamoDBMock, TABLE_NAME, INTEGRATION_TESTER).withLeaseDuration(SHORT_LEASE_DUR)
                        .withHeartbeatPeriod(10L).withTimeUnit(TimeUnit.MILLISECONDS).withCreateHeartbeatBackgroundThread(true).build()));
        this.lockClientForRangeKeyTable = Mockito.spy(new AmazonDynamoDBLockClient(
                new AmazonDynamoDBLockClientOptions.AmazonDynamoDBLockClientOptionsBuilder(this.dynamoDBMock, RANGE_KEY_TABLE_NAME, INTEGRATION_TESTER).withLeaseDuration(3L).withHeartbeatPeriod(1L)
                        .withTimeUnit(TimeUnit.SECONDS).withSortKeyName("rangeKey").withCreateHeartbeatBackgroundThread(false).build()));
        this.lockClientWithHeartbeatingForRangeKeyTable = Mockito.spy(new AmazonDynamoDBLockClient(
                new AmazonDynamoDBLockClientOptions.AmazonDynamoDBLockClientOptionsBuilder(this.dynamoDBMock, RANGE_KEY_TABLE_NAME, INTEGRATION_TESTER).withLeaseDuration(3L).withHeartbeatPeriod(1L)
                        .withTimeUnit(TimeUnit.SECONDS).withSortKeyName("rangeKey").withCreateHeartbeatBackgroundThread(true).build()));
        this.shortLeaseLockClientForRangeKeyTable = Mockito.spy(new AmazonDynamoDBLockClient(
                new AmazonDynamoDBLockClientOptions.AmazonDynamoDBLockClientOptionsBuilder(this.dynamoDBMock, RANGE_KEY_TABLE_NAME, INTEGRATION_TESTER).withLeaseDuration(SHORT_LEASE_DUR)
                        .withHeartbeatPeriod(10L).withTimeUnit(TimeUnit.MILLISECONDS).withSortKeyName("rangeKey").withCreateHeartbeatBackgroundThread(false).build()));
    }

    public DynamoDbClient safelySpyDDB(final DynamoDbClient dynamoDB) {
        //Replaces Mockito.spy(this.dynamoDBMock) and works for proxies as well (i.e. dynamodb local embedded)
        return Mockito.mock(DynamoDbClient.class, delegatesTo(dynamoDB));
    }

    @After
    public void deleteTables() {
        this.dynamoDBMock.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
        this.dynamoDBMock.deleteTable(DeleteTableRequest.builder().tableName(RANGE_KEY_TABLE_NAME).build());
    }

    protected final String byteBufferToString(ByteBuffer buffer) {
        return byteBufferToString(buffer, Charset.defaultCharset());
    }
    // https://stackoverflow.com/questions/1252468/java-converting-string-to-and-from-bytebuffer-and-associated-problems
    private final String byteBufferToString(ByteBuffer buffer, Charset charset) {
        return new String(getBytes(buffer), charset);
    }

    protected byte[] getBytes(ByteBuffer buffer) {
        byte[] bytes;
        if (buffer.hasArray()) {
            bytes = buffer.array();
        } else {
            bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
        }
        return bytes;
    }
}
