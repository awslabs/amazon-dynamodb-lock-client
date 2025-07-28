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

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * ReleaseLockOptions unit tests.
 *
 * @author <a href="mailto:amcp@amazon.com">Alexander Patrikalakis</a> 2017-07-13
 */
@RunWith(MockitoJUnitRunner.class)
public class ReleaseLockOptionsTest {
    @Mock
    AmazonDynamoDBLockClient lockClient;
    @Test
    public void withLockItem_setsLockItem() {
        LockItem lockItem = LockItemTest.createLockItem(lockClient);
        ReleaseLockOptions.ReleaseLockOptionsBuilder builder = ReleaseLockOptions.builder(lockItem);
        System.out.println(builder.toString());
        assertEquals(lockItem, builder.build().getLockItem());
    }
}
