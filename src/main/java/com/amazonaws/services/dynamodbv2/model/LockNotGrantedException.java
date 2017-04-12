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
