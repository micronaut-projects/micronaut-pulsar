/*
 * Copyright 2017-2021 original authors
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

import io.micronaut.context.annotation.Requires;
import io.micronaut.inject.ExecutableMethod;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;

/**
 * Default listener for incoming Pulsar messages.
 *
 * @author Haris Secic
 * @since 1.0
 */
@Requires(missingBeans = MessageListenerResolver.class)
@SuppressWarnings({"rawtypes", "unchecked"})
public class DefaultListener implements MessageListenerResolver {

    private final boolean useMessageWrapper;
    private final ExecutableMethod<Object, ?> method;
    private final int consumerIndex;
    private final Object invoker;

    public DefaultListener(ExecutableMethod method, boolean useMessageWrapper,
                           int consumerIndex, Object invoker) {
        this.method = method;
        this.useMessageWrapper = useMessageWrapper;
        this.consumerIndex = consumerIndex;
        this.invoker = invoker;
    }

    @Override
    public void received(Consumer consumer, Message msg) {
        Object any;
        if (useMessageWrapper) {
            any = msg;
        } else {
            any = msg.getValue();
        }
        //trying to provide more flexibility to developers by allowing less care about the order; maybe unnecessary?
        switch (consumerIndex) {
            case 0:
                method.invoke(invoker, consumer, any);
                break;
            case 1:
                method.invoke(invoker, any, consumer);
                break;
            default:
                method.invoke(invoker, any);
        }
        //in case invoke has failed but async ack will still happen instead of negative ack
        consumer.acknowledgeAsync(msg);
    }
}
