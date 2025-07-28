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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Used to verify consistent lock data functionality in multi-threaded lock environment
 */
public class ConsistentLockDataStressTest extends InMemoryLockClientTester {
    private static final Log logger = LogFactory.getLog(ConsistentLockDataStressTest.class);

    @Test
    public void consistentDataUsingUpdateLockRecord() throws InterruptedException, IOException, ExecutionException {
        final int numOfThreads = 50;
        final int maxWorkDoneMillis = 100;
        final int numRepetitions = 10;

        final LockItem initalLock = lockClientWithHeartbeating.acquireLock(AcquireLockOptions.builder(
                "consistentDataUsingUpdateLockRecordKey").withAdditionalTimeToWaitForLock(Long.MAX_VALUE / 2).withAcquireReleasedLocksConsistently(true)
                .withReplaceData(false).withUpdateExistingLockRecord(true).withDeleteLockOnRelease(false).withRefreshPeriod(10L)
                .withTimeUnit(TimeUnit.MILLISECONDS).build());

        lockClientWithHeartbeating.releaseLock(ReleaseLockOptions.builder(initalLock).withDeleteLock(false).withData(putLockData(0))
                .withBestEffort(false).build());

        final Runnable runnable = () -> {
            Optional<LockItem> lockItem = Optional.empty();
            Integer count = 0;
            try {
                lockItem = lockClientWithHeartbeating.tryAcquireLock(AcquireLockOptions.builder("consistentDataUsingUpdateLockRecordKey")
                        .withAcquireOnlyIfLockAlreadyExists(true).withAdditionalTimeToWaitForLock(Long.MAX_VALUE / 2).withAcquireReleasedLocksConsistently(
                                true).withReplaceData(false).withUpdateExistingLockRecord(true).withDeleteLockOnRelease(false)
                        .withRefreshPeriod(10L).withTimeUnit(TimeUnit.MILLISECONDS).build());
                assertNotEquals(Optional.empty(), lockItem);
                count = getLockData(lockItem.get()) + 1;
                Thread.sleep(getRandomNumberInRange(1, maxWorkDoneMillis));
            } catch (final InterruptedException e) {
                fail("Should not get interrupted!");
            } finally {
                if (lockItem.isPresent()) {
                    lockClientWithHeartbeating.releaseLock(ReleaseLockOptions.builder(lockItem.get()).withDeleteLock(false).withData(
                            putLockData(count)).withBestEffort(false).build());
                }
            }
        };

        assertConcurrent("consistentDataUsingUpdateLockRecord", Collections.nCopies(numOfThreads, runnable), 600, numRepetitions);

        final Optional<LockItem> lockItem = lockClientWithHeartbeating.tryAcquireLock(AcquireLockOptions.builder(
                "consistentDataUsingUpdateLockRecordKey").withAdditionalTimeToWaitForLock(Long.MAX_VALUE / 2).withAcquireReleasedLocksConsistently(true)
                .withReplaceData(false).withUpdateExistingLockRecord(true).withDeleteLockOnRelease(true).withRefreshPeriod(10L)
                .withTimeUnit(TimeUnit.MILLISECONDS).build());
        assertNotEquals(Optional.empty(), lockItem);
        assertEquals(numOfThreads*numRepetitions, getLockData(lockItem.get()).intValue());
        lockItem.get().close();
    }

    @Test
    public void notConsistentLockDataUsingUpdateLockRecord() throws InterruptedException, IOException, ExecutionException {
        final int numOfThreads = 50;
        final int maxWorkDoneMillis = 100;
        final int numRepetitions = 10;

        final LockItem initalLock = lockClientWithHeartbeating.acquireLock(AcquireLockOptions.builder(
                "notConsistentLockDataUsingUpdateLockRecordKey").withAdditionalTimeToWaitForLock(Long.MAX_VALUE / 2).withAcquireReleasedLocksConsistently(false)
                .withReplaceData(false).withUpdateExistingLockRecord(true).withDeleteLockOnRelease(false).withRefreshPeriod(10L)
                .withTimeUnit(TimeUnit.MILLISECONDS).build());

        lockClientWithHeartbeating.releaseLock(ReleaseLockOptions.builder(initalLock).withDeleteLock(false).withData(putLockData(0))
                .withBestEffort(false).build());

        final Runnable runnable = () -> {
            Optional<LockItem> lockItem = Optional.empty();
            Integer count = 0;
            try {
                lockItem = lockClientWithHeartbeating.tryAcquireLock(AcquireLockOptions.builder("notConsistentLockDataUsingUpdateLockRecordKey")
                        .withAcquireOnlyIfLockAlreadyExists(true).withAdditionalTimeToWaitForLock(Long.MAX_VALUE / 2).withAcquireReleasedLocksConsistently(
                                false).withReplaceData(false).withUpdateExistingLockRecord(true).withDeleteLockOnRelease(false)
                        .withRefreshPeriod(10L).withTimeUnit(TimeUnit.MILLISECONDS).build());
                assertNotEquals(Optional.empty(), lockItem);
                count = getLockData(lockItem.get()) + 1;
                Thread.sleep(getRandomNumberInRange(1, maxWorkDoneMillis));
            } catch (final InterruptedException e) {
                fail("Should not get interrupted!");
            } finally {
                if (lockItem.isPresent()) {
                    lockClientWithHeartbeating.releaseLock(ReleaseLockOptions.builder(lockItem.get()).withDeleteLock(false).withData(
                            putLockData(count)).withBestEffort(false).build());
                }
            }
        };

        assertConcurrent("notConsistentLockDataUsingUpdateLockRecord", Collections.nCopies(numOfThreads, runnable), 600, numRepetitions);

        final Optional<LockItem> lockItem = lockClientWithHeartbeating.tryAcquireLock(AcquireLockOptions.builder(
                "notConsistentLockDataUsingUpdateLockRecordKey").withAdditionalTimeToWaitForLock(Long.MAX_VALUE / 2).withAcquireReleasedLocksConsistently(false)
                .withReplaceData(false).withUpdateExistingLockRecord(true).withDeleteLockOnRelease(true).withRefreshPeriod(10L)
                .withTimeUnit(TimeUnit.MILLISECONDS).build());
        assertNotEquals(Optional.empty(), lockItem);
        assertNotEquals(numOfThreads*numRepetitions, getLockData(lockItem.get()).intValue());
        lockItem.get().close();
    }

    @Test
    public void consistentDataUsingPutLockRecord() throws InterruptedException, IOException, ExecutionException {
        final int numOfThreads = 50;
        final int maxWorkDoneMillis = 100;
        final int numRepetitions = 10;

        final LockItem initalLock = lockClientWithHeartbeating.acquireLock(AcquireLockOptions.builder("consistentDataUsingPutLockRecordKey")
                .withAdditionalTimeToWaitForLock(Long.MAX_VALUE / 2).withAcquireReleasedLocksConsistently(true).withReplaceData(false)
                .withUpdateExistingLockRecord(false).withDeleteLockOnRelease(false).withRefreshPeriod(10L)
                .withTimeUnit(TimeUnit.MILLISECONDS).build());

        lockClientWithHeartbeating.releaseLock(ReleaseLockOptions.builder(initalLock).withDeleteLock(false).withData(putLockData(0))
                .withBestEffort(false).build());

        final Runnable runnable = () -> {
            Optional<LockItem> lockItem = Optional.empty();
            Integer count = 0;
            try {
                lockItem = lockClientWithHeartbeating.tryAcquireLock(AcquireLockOptions.builder("consistentDataUsingPutLockRecordKey")
                        .withAcquireOnlyIfLockAlreadyExists(true).withAdditionalTimeToWaitForLock(Long.MAX_VALUE / 2).withAcquireReleasedLocksConsistently(
                                true).withReplaceData(false).withUpdateExistingLockRecord(false).withDeleteLockOnRelease(false)
                        .withRefreshPeriod(10L).withTimeUnit(TimeUnit.MILLISECONDS).build());
                assertNotEquals(Optional.empty(), lockItem);
                count = getLockData(lockItem.get()) + 1;
                Thread.sleep(getRandomNumberInRange(1, maxWorkDoneMillis));
            } catch (final InterruptedException e) {
                fail("Should not get interrupted!");
            } finally {
                if (lockItem.isPresent()) {
                    lockClientWithHeartbeating.releaseLock(ReleaseLockOptions.builder(lockItem.get()).withDeleteLock(false).withData(
                            putLockData(count)).withBestEffort(false).build());
                }
            }
        };

        assertConcurrent("consistentDataUsingPutLockRecord", Collections.nCopies(numOfThreads, runnable), 600, numRepetitions);

        final Optional<LockItem> lockItem = lockClientWithHeartbeating.tryAcquireLock(AcquireLockOptions.builder(
                "consistentDataUsingPutLockRecordKey").withAdditionalTimeToWaitForLock(Long.MAX_VALUE / 2).withAcquireReleasedLocksConsistently(true)
                .withReplaceData(false).withUpdateExistingLockRecord(false).withDeleteLockOnRelease(true).withRefreshPeriod(10L)
                .withTimeUnit(TimeUnit.MILLISECONDS).build());
        assertNotEquals(Optional.empty(), lockItem);
        assertEquals(numOfThreads*numRepetitions, getLockData(lockItem.get()).intValue());
        lockItem.get().close();
    }

    @Test
    public void notConsistentLockDataUsingPutLockRecord() throws InterruptedException, IOException, ExecutionException {
        final int numOfThreads = 50;
        final int maxWorkDoneMillis = 100;
        final int numRepetitions = 10;

        final LockItem initalLock = lockClientWithHeartbeating.acquireLock(AcquireLockOptions.builder("notConsistentLockDataUsingPutLockRecordKey")
                .withAdditionalTimeToWaitForLock(Long.MAX_VALUE / 2).withAcquireReleasedLocksConsistently(false).withReplaceData(false)
                .withUpdateExistingLockRecord(false).withDeleteLockOnRelease(false).withRefreshPeriod(10L)
                .withTimeUnit(TimeUnit.MILLISECONDS).build());

        lockClientWithHeartbeating.releaseLock(ReleaseLockOptions.builder(initalLock).withDeleteLock(false).withData(putLockData(0))
                .withBestEffort(false).build());

        final Runnable runnable = () -> {
            Optional<LockItem> lockItem = Optional.empty();
            Integer count = 0;
            try {
                lockItem = lockClientWithHeartbeating.tryAcquireLock(AcquireLockOptions.builder("notConsistentLockDataUsingPutLockRecordKey")
                        .withAcquireOnlyIfLockAlreadyExists(true).withAdditionalTimeToWaitForLock(Long.MAX_VALUE / 2).withAcquireReleasedLocksConsistently(
                                false).withReplaceData(false).withUpdateExistingLockRecord(false).withDeleteLockOnRelease(false)
                        .withRefreshPeriod(10L).withTimeUnit(TimeUnit.MILLISECONDS).build());
                assertNotEquals(Optional.empty(), lockItem);
                count = getLockData(lockItem.get()) + 1;
                Thread.sleep(getRandomNumberInRange(1, maxWorkDoneMillis));
            } catch (final InterruptedException e) {
                fail("Should not get interrupted!");
            } finally {
                if (lockItem.isPresent()) {
                    lockClientWithHeartbeating.releaseLock(ReleaseLockOptions.builder(lockItem.get()).withDeleteLock(false).withData(
                            putLockData(count)).withBestEffort(false).build());
                }
            }
        };

        assertConcurrent("notConsistentLockDataUsingPutLockRecord", Collections.nCopies(numOfThreads, runnable), 600, numRepetitions);

        final Optional<LockItem> lockItem = lockClientWithHeartbeating.tryAcquireLock(AcquireLockOptions.builder(
                "notConsistentLockDataUsingPutLockRecordKey").withAdditionalTimeToWaitForLock(Long.MAX_VALUE / 2).withAcquireReleasedLocksConsistently(false)
                .withReplaceData(false).withUpdateExistingLockRecord(false).withDeleteLockOnRelease(true).withRefreshPeriod(10L)
                .withTimeUnit(TimeUnit.MILLISECONDS).build());
        assertNotEquals(Optional.empty(), lockItem);
        assertNotEquals(numOfThreads*numRepetitions, getLockData(lockItem.get()).intValue());
        lockItem.get().close();
    }

    private static Integer getLockData(final LockItem lockItem) {
        return Integer.valueOf(new String(lockItem.getData().orElse(ByteBuffer.wrap("0".getBytes())).array()));
    }

    private static ByteBuffer putLockData(final Integer count) {
        return ByteBuffer.wrap(String.valueOf(count).getBytes());
    }

    private static void assertConcurrent(final String message, final Collection<? extends Runnable> runnables, final int maxTimeoutSeconds,
                                         final int numOfRepetitions) throws InterruptedException {
        assertTrue(message + " Number of repetitions should be greater than 0", numOfRepetitions > 0);
        final int numThreads = runnables.size();
        final List<Throwable> exceptions = Collections.synchronizedList(new ArrayList<>());
        final ExecutorService threadPool = Executors.newFixedThreadPool(numThreads);
        try {
            final CountDownLatch allExecutorThreadsReady = new CountDownLatch(numThreads);
            final CountDownLatch afterInitBlocker = new CountDownLatch(1);
            final CountDownLatch allDone = new CountDownLatch(numThreads);
            for (final Runnable submittedTestRunnable : runnables) {
                threadPool.submit(() -> {
                    allExecutorThreadsReady.countDown();
                    try {
                        afterInitBlocker.await();
                        repeat(submittedTestRunnable, numOfRepetitions);
                    } catch (final Throwable e) {
                        exceptions.add(e);
                    } finally {
                        allDone.countDown();
                    }
                });
            }
            // wait until all threads are ready
            assertTrue(
                    message + " Timeout initializing threads! Perform long lasting initializations before passing runnables to " +
                            "assertConcurrent",
                    allExecutorThreadsReady.await(runnables.size() * 10, TimeUnit.MILLISECONDS));
            // start all test runners
            afterInitBlocker.countDown();
            assertTrue(message + " Timeout! More than" + maxTimeoutSeconds + "seconds", allDone.await(maxTimeoutSeconds, TimeUnit.SECONDS));
        } finally {
            threadPool.shutdownNow();
        }

        if (!exceptions.isEmpty()) {
            for (final Throwable e : exceptions) {
                logger.error(e.getMessage(), e);
            }
        }
        assertTrue(message + " failed with exception(s)" + exceptions, exceptions.isEmpty());
    }

    private static void repeat(final Runnable action, final int numOfRepetitions) {
        for (int i = 0; i < numOfRepetitions; i++) {
            action.run();
        }
    }

    private static int getRandomNumberInRange(final int min, final int max) {
        final Random r = new Random();
        return r.ints(min, (max + 1)).limit(1).findFirst().getAsInt();
    }
}
