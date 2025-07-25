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
 * This is a retry-able exception, that indicates that the lock being requested has already been held by another worker
 * and has not been released yet and the lease duration has not expired since the lock was last updated by the current
 * owner.
 *
 * The caller can retry acquiring the lock with or without a backoff.
 *
 * @author <a href="mailto:sath@amazon.com">Sathish kumar AC</a>
 */
public class LockCurrentlyUnavailableException extends RuntimeException {

    private static final long serialVersionUID = 661782974798613851L;

    public LockCurrentlyUnavailableException() {
    }

    public LockCurrentlyUnavailableException(String message) {
        super(message);
    }

    public LockCurrentlyUnavailableException(Throwable cause) {
        super(cause);
    }

    public LockCurrentlyUnavailableException(String message,
            Throwable cause) {
        super(message, cause);
    }

    public LockCurrentlyUnavailableException(String message,
            Throwable cause,
            boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

}
