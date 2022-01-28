# Amazon DynamoDB Lock Client

The Amazon DynamoDB Lock Client is a general purpose distributed locking library
built for DynamoDB. The DynamoDB Lock Client supports both fine-grained and
coarse-grained locking as the lock keys can be any arbitrary string, up to a
certain length. DynamoDB Lock Client is an open-source project that will be
supported by the community. Please create issues in the GitHub repository with
questions.

[![Build Status](https://travis-ci.org/awslabs/dynamodb-lock-client.svg?branch=master)](https://travis-ci.org/awslabs/dynamodb-lock-client)

# Warning / Clarification: 
Only v1.1.0 has been released to Maven. We recommend if you are are consuming from Maven to refer to the [1.1.x branch we have](https://github.com/awslabs/amazon-dynamodb-lock-client/tree/v1.1.x). v1.2.x is not yet released, containing SDKV2 changes, and so-on. 

## Use cases
A common use case for this lock client is:
let's say you have a distributed system that needs to periodically do work on a given campaign
(or a given customer, or any other object) and you want to make sure that two boxes don't work
on the same campaign/customer at the same time. An easy way to fix this is to write a system that takes
a lock on a customer, but fine-grained locking is a tough problem. This library attempts to simplify
this locking problem on top of DynamoDB.

Another use case is leader election. If you only want one host to be the leader, then this lock
client is a great way to pick one. When the leader fails, it will fail over to another host
within a customizable leaseDuration that you set.

## Getting Started
To use the Amazon DynamoDB Lock Client, declare a dependency on the latest version of
this artifact in Maven in your pom.xml.
```xml
<dependency>
    <groupId>com.amazonaws</groupId>
    <artifactId>dynamodb-lock-client</artifactId>
    <version>1.2.0</version>
</dependency>
```

Then, you need to set up a DynamoDB table that has a hash key on a key with the name `key`.
For your convenience, there is a static method in the AmazonDynamoDBLockClient class called
`createLockTableInDynamoDB` that you can use to set up your table, but it is also possible to set
up the table in the AWS Console. The table should be created in advance, since it takes a couple minutes
for DynamoDB to provision your table for you. The AmazonDynamoDBLockClient has JavaDoc comments that fully
explain how the library works. Here is some example code to get you started:

```java
import java.io.IOException;
import java.net.URI;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public class LockClientExample {
    @Test
    public void usageExample() throws InterruptedException, IOException {
        // Inject client configuration to the builder like the endpoint and signing region
        final DynamoDbClient dynamoDB = DynamoDbClient.builder()
                .region(Region.US_WEST_2).endpointOverride(URI.create("http://localhost:4567"))
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
```

#### Permissions
The following permissions are required in order to use the AmazonDynamoDBLockClient to acquire or release a lock:
- `dynamodb:DeleteItem`
- `dynamodb:GetItem`
- `dynamodb:PutItem`
- `dynamodb:Scan`
- `dynamodb:UpdateItem`

If you also create a table using `AmazonDynamoDBLockClient.createLockTableInDynamoDB`, you need these permissions:
- `dynamodb:CreateTable`

If you want to ensure a table exists using `AmazonDynamoDBLockClient.assertLockTableExists` or
`AmazonDynamoDBLockClient.lockTableExists`, the following permissions are necessary as well:
- `dynamodb:DescribeTable`


## Selected Features
### Send Automatic Heartbeats
When you call the constructor AmazonDynamoDBLockClient, you can specify `createHeartbeatBackgroundThread=true`
like in the above example, and it will spawn a background thread that continually updates the record version
number on your locks to prevent them from expiring (it does this by calling the sendHeartbeat() method in the
lock client.) This will ensure that as long as your JVM is running, your locks will not expire until you call
releaseLock() or lockItem.close()

### Acquire lock with timeout
You can acquire a lock via two different methods: acquireLock or tryAcquireLock. The difference between the
two methods is that tryAcquireLock will return Optional.absent() if the lock was not acquired, whereas
acquireLock will throw a LockNotGrantedException. Both methods provide optional parameters where you can specify
an additional timeout for acquiring the lock. Then they will try to acquire the lock for that amount of time
before giving up. They do this by continually polling DynamoDB according to an interval you set up. Remember that
acquireLock/tryAcquireLock will always poll DynamoDB for at least the leaseDuration period before giving up,
because this is the only way it will be able to expire stale locks.

This example will poll DynamoDB every second for 5 additional seconds (beyond the lease duration period),
trying to acquire a lock:
```groovy
LockItem lock = lockClient.acquireLock("Moe", "Test Data", 1, 5, TimeUnit.SECONDS);
```
### Acquire lock without blocking the user thread.
Example Use Case:
 Suppose you have many messages that need to be processed for multiple lockable entities by a limited set of
 processor-consumers. Further suppose that the processing time for each message is significant (for example, 15 minutes).
 You also need to prevent multiple processing for the same resource.

```
  @Test
    public void acquireLockNonBlocking() throws LockAlreadyOwnedException {
        AcquireLockOptions lockOptions = AcquireLockOptions.builder("partitionKey")
                                        .withShouldSkipBlockingWait(false).build();
        LockItem lock = lockClient.acquireLock(lockOptions);
    }
```
The above implementation of the locking client, would try to acquire lock, waiting for at least the lease duration (15
minutes in our case). If the lock is already being held by other worker. This essentially blocks the threads from being
used to process other messages in the queue.

So we introduced an optional behavior which offers a Non-Blocking acquire lock implementation. While trying to acquire
lock, the client can now optionally set `shouldSkipBlockingWait = true` to prevent the user thread from being
blocked until the lease duration, if the lock has already been held by another worker and has not been released yet.
The caller can chose to immediately retry the lock acquisition or to back off and retry the lock acquisition, if lock is
currently unavailable.

```
    @Test
    public void acquireLockNonBlocking() throws LockAlreadyOwnedException {
        AcquireLockOptions lockOptions = AcquireLockOptions.builder("partitionKey")
                                        .withShouldSkipBlockingWait(true).build();
        LockItem lock = lockClient.acquireLock(lockOptions);
    }
```
If the lock does not exist or if the lock has been acquired by the other machine and is stale (has passed the lease
duration), this would successfully acquire the lock.

If the lock has already been held by another worker and has not been released yet and the lease duration has not expired
since the lock was last updated by the current owner, this will throw a LockCurrentlyUnavailableException exception.
The caller can chose to immediately retry the lock acquisition or to delay the processing for that lock item by NACKing
the message.

### Read the data in a lock without acquiring it
You can read the data in the lock without acquiring it, and find out who owns the lock. Here's how:
```groovy
LockItem lock = lockClient.getLock("Moe");
```

## How we handle clock skew
The lock client never stores absolute times in DynamoDB -- only the relative "lease duration" time is stored
in DynamoDB. The way locks are expired is that a call to acquireLock reads in the current lock, checks the
RecordVersionNumber of the lock (which is a GUID) and starts a timer. If the lock still has the same GUID after
the lease duration time has passed, the client will determine that the lock is stale and expire it.
What this means is that, even if two different machines disagree about what time it is, they will still avoid
clobbering each other's locks.

## Testing the DynamoDB Locking client
To run all integration tests for the DynamoDB Lock client, issue the following Maven command:

```bash
mvn clean install -Pintegration-tests
```

# External release to Sonatype
```bash
mvn deploy -Possrh-release
```
