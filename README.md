# Amazon DynamoDB Lock Client

The Amazon DynamoDB Lock Client is a general purpose distributed locking library
built for DynamoDB. The DynamoDB Lock Client supports both fine-grained and
coarse-grained locking as the lock keys can be any arbitrary string, up to a
certain length. DynamoDB Lock Client is an open-source project that will be
supported by the community. Please create issues in the GitHub repository with
questions.

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
    <version>1.0.0</version>
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

```

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

### Read the data in a lock without acquiring it
You can read the data in the lock without acquiring it, and find out who owns the lock. Here's how:
```groovy
LockItem lock = lockClient.getLock("Moe");
```

### Fencing
By default the lock client relies on lock holders to ensure that they have finished all of their work before their
lease expires. This can be [hard to guarantee](https://fpj.me/2016/02/10/note-on-fencing-and-distributed-locks/)
when you account for things like thread schedulers, garbage collectors, and network latency. To protect against
corruption in the event that a request from an "old" lock holder arrives after its lease has expired and the lock
has been acquired by a different system, the lock client can optionally associate a unique, monotonically-increasing
**sequence id** with each lease:

```java
AmazonDynamoDBLockClient client = new AmazonDynamoDBLockClient(
    AmazonDynamoDBLockClientOptions.builder(dynamoDB, "lockTable")
            .withSequenceIdTracking(true)
            .withLeaseDuration(10L)
            .withTimeUnit(TimeUnit.SECONDS)
            .build());

try (LockItem lock = client.acquireLock(AcquireLockOptions.builder("GloriousLeader").build())) {
    // Simulate a particularly nasty garbage collection pause.
    Thread.sleep(30 * 1000);

    // Send a message to all of our followers; they can ignore it if they've already heard from a
    // leader with a later sequence id.
    for (String follower : getFollowers()) {
        sendMessage(follower, "Hello from leader " + lock.getSequenceId() + "!");
    }
}
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
mvn clean install verify -Pintegration-tests
```
