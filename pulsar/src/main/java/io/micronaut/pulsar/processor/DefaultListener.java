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

import io.micronaut.inject.DelegatingExecutableMethod;
import io.micronaut.inject.ExecutableMethod;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Default listener for incoming Pulsar messages.
 *
 * @author Haris Secic
 * @since 1.0
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class DefaultListener implements MessageListenerResolver {

    private final Logger LOGGER = LoggerFactory.getLogger(DefaultListener.class);

    private final ExecutableMethod<Object, ?> method;
    private final BiConsumer<Consumer<?>, Message<?>> receive;

    public DefaultListener(final ExecutableMethod method,
                           final boolean useMessageWrapper,
                           final Object invoker,
                           final PulsarArgumentHandler argumentHandler) {
        this.method = method;
        final boolean isSuspend;
        if (method instanceof DelegatingExecutableMethod) {
            isSuspend = ((DelegatingExecutableMethod) method).getTarget().isSuspend();
        } else {
            isSuspend = method.isSuspend();
        }
        final Map<String, Integer> headersOrder = argumentHandler.headersOrder();
        final Map<String, Integer> argsOrder = argumentHandler.argumentOrder();
        final int totalArgs = argumentHandler.size();
        final boolean hasHeadersAsMap = argumentHandler.hasHeadersMap();
        receive = (c, v) -> {
            final Object[] params = new Object[totalArgs];
            params[argsOrder.get("body")] = useMessageWrapper ? v : v.getValue();
            if (argsOrder.containsKey("consumer")) {
                params[argsOrder.get("consumer")] = c;
            }
            if (argsOrder.containsKey("key")) {
                params[argsOrder.get("key")] = v.getKey();
            }
            if (hasHeadersAsMap) {
                params[argsOrder.get("headers")] = v.getProperties();
            } else {
                headersOrder.keySet().forEach(x -> params[headersOrder.get(x)] = v.getProperties().get(x));
            }
            if (isSuspend) {
                ListenerKotlinHelper.run(method, invoker, params);
            } else {
                method.invoke(invoker, params);
            }
        };
    }

    //Pulsar Java lib uses CompletableFutures and has no context/continuation upon the arrival of the message
    //so in case of Kotlin we need to create those and do a blocking call within this CompletableFuture

    @Override
    public void received(final Consumer consumer, final Message msg) {
        try {
            receive.accept(consumer, msg);
            consumer.acknowledgeAsync(msg);
        } catch (Exception ex) {
            consumer.negativeAcknowledge(msg.getMessageId());
            LOGGER.error("Could not parse message [{}] for [{}] on method [{}]", msg.getMessageId(), consumer.getConsumerName(), method.getName(), ex);
        }
    }

}
