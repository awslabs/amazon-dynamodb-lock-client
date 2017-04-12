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
 * Thrown to indicate that SessionMonitor is not set.
 *
 * @author <a href="mailto:joserock@amazon.com">Joseph Rock</a>
 */
public class SessionMonitorNotSetException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    /**
     * Basic constructor.
     *
     * @param message content to show
     */
    public SessionMonitorNotSetException(final String message) {
        super(message);
    }

    /**
     * Full constructor.
     *
     * @param message content to show
     * @param cause   Root cause
     */
    public SessionMonitorNotSetException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
