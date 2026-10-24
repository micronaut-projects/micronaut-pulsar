/*
 * Copyright 2017-2022 original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.micronaut.pulsar.processor;

import java.util.Random;
import org.apache.pulsar.client.api.RedeliveryBackoff;
import com.google.common.base.Preconditions;

public class ExponentialRedeliveryBackoff implements RedeliveryBackoff {

    static Random random = new Random();

    private final long firstBackoffInMs;
    private final long maxDelayInMs;

    public ExponentialRedeliveryBackoff(long firstBackoffInMs, long maxDelayInMs) {
        Preconditions.checkArgument(firstBackoffInMs > 0, "firstBackoff is not allowed to be negative");
        Preconditions.checkArgument(maxDelayInMs >= 0, "maxDelayInMs is not allowed to be negative");
        Preconditions.checkArgument(maxDelayInMs > firstBackoffInMs, "maxDelayInMs should be greater than firstBackoffInMs");
        this.firstBackoffInMs = firstBackoffInMs;
        this.maxDelayInMs = maxDelayInMs;
    }

    /**
     * Decorrelated Jitter, see https://aws.amazon.com/fr/blogs/architecture/exponential-backoff-and-jitter/
     * @param redeliveryCount indicates the number of times the message was redelivered
     * @return delay in milliseconds
     */
    @Override
    public long next(int redeliveryCount) {
        double exponentialDelay = firstBackoffInMs * Math.pow(2, redeliveryCount);
        return (long) Math.min( maxDelayInMs, firstBackoffInMs + random.nextDouble() * exponentialDelay);
    }
}
