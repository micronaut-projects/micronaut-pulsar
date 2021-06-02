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
import io.micronaut.core.type.Argument;
import io.micronaut.inject.ExecutableMethod;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Default listener for incoming Pulsar messages.
 *
 * @author Haris Secic
 * @since 1.0
 */
@Requires(missingBeans = MessageListenerResolver.class)
@SuppressWarnings({"rawtypes", "unchecked"})
public class DefaultListener implements MessageListenerResolver {

    private final Logger LOGGER = LoggerFactory.getLogger(DefaultListener.class);

    private final boolean isSuspend;
    private final boolean useMessageWrapper;
    private final ExecutableMethod<Object, ?> method;
    private final int consumerIndex;
    private final Object invoker;

    public DefaultListener(ExecutableMethod method,
                           boolean useMessageWrapper,
                           Object invoker) {
        this.method = method;
        this.useMessageWrapper = useMessageWrapper;
        this.invoker = invoker;
        Argument[] args = method.getArguments();
        this.isSuspend = method.getReturnType().isSuspended();
        this.consumerIndex = IntStream.range(0, args.length)
                .filter(i -> Consumer.class.isAssignableFrom(args[i].getType()))
                .findFirst().orElse(-1);
    }

    @Override
    public void received(Consumer consumer, Message msg) {
        Object any;
        try {
            any = useMessageWrapper ? msg : msg.getValue(); // .getValue can hit the serialisation exception
            //trying to provide more flexibility to developers by allowing less care about the order; maybe unnecessary
            Object[] args = args(any, consumer);
            method.invoke(invoker, args);
            consumer.acknowledgeAsync(msg);
        } catch (Exception ex) {
            consumer.negativeAcknowledge(msg.getMessageId());
            LOGGER.error("Could not parse message [{}] for [{}] on method [{}]", msg.getMessageId(), consumer.getConsumerName(), method.getName());
        }
    }

    private Object[] args(Object value, Object consumer) {
        List<Object> args = new ArrayList<>(3);
        switch (consumerIndex) {
            case 0:
                args.add(consumer);
                args.add(value);
                break;
            case 1:
                args.add(value);
                args.add(consumer);
                break;
            default:
                args.add(value);
        }
        if (isSuspend) {
            args.add(null);
        }
        return args.toArray();
    }
}
