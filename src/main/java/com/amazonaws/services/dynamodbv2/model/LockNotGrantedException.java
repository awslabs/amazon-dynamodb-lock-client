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
package com.amazonaws.services.dynamodbv2.model;

/**
 * Thrown to indicate that a caller was not granted a lock that it requested.
 *
 * @author <a href="mailto:slutsker@amazon.com">Sasha Slutsker</a>
 */
public class LockNotGrantedException extends RuntimeException {

    private static final long serialVersionUID = 5911380235050075961L;

    public LockNotGrantedException() {
        super();
    }

    public LockNotGrantedException(final String message) {
        super(message);
    }

    public LockNotGrantedException(final String message, final Throwable exception) {
        super(message, exception);
    }
}
