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
import io.micronaut.inject.DelegatingExecutableMethod;
import io.micronaut.inject.ExecutableMethod;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiConsumer;
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

    private final boolean useMessageWrapper;
    private final ExecutableMethod<Object, ?> method;
    private final BiConsumer<Object, Object> receive;

    public DefaultListener(ExecutableMethod method, boolean useMessageWrapper, Object invoker) {
        this.method = method;
        this.useMessageWrapper = useMessageWrapper;
        boolean isSuspend;
        if (method instanceof DelegatingExecutableMethod) {
            isSuspend = ((DelegatingExecutableMethod) method).getTarget().isSuspend();
        } else {
            isSuspend = method.isSuspend();
        }
        Argument[] args = method.getArguments();
        int consumerIndex = IntStream.range(0, args.length)
                .filter(i -> Consumer.class.isAssignableFrom(args[i].getType()))
                .findFirst().orElse(-1);
        switch (consumerIndex) {
            case 0:
                if (isSuspend) {
                    receive = (c, v) -> ListenerKotlinHelper.run(method, invoker, c, v);
                } else {
                    receive = (c, v) -> method.invoke(invoker, c, v);
                }
                break;
            case 1:
                if (isSuspend) {
                    receive = (c, v) -> ListenerKotlinHelper.run(method, invoker, v, c);
                } else {
                    receive = (c, v) -> method.invoke(invoker, v, c);
                }
                break;
            default:
                if (!isSuspend) {
                    receive = (c, v) -> method.invoke(invoker, v);
                } else {
                    receive = (c, v) -> ListenerKotlinHelper.run(method, invoker, v);
                }
        }
    }

    //Pulsar Java lib uses CompletableFutures and has no context/continuation upon the arrival of the message
    //so in case of Kotlin we need to create those and do a blocking call within this CompletableFuture

    @Override
    public void received(Consumer consumer, Message msg) {
        try {
            // .getValue can hit the serialisation exception
            Object any = useMessageWrapper ? msg : msg.getValue();
            //trying to provide more flexibility to developers by allowing less care about the order; maybe unnecessary
            receive.accept(consumer, any);
            consumer.acknowledgeAsync(msg);
        } catch (Exception ex) {
            consumer.negativeAcknowledge(msg.getMessageId());
            LOGGER.error("Could not parse message [{}] for [{}] on method [{}]", msg.getMessageId(), consumer.getConsumerName(), method.getName(), ex);
        }
    }

}
