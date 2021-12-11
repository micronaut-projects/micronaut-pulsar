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

import io.micronaut.core.annotation.AnnotationValue;
import io.micronaut.core.annotation.Internal;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.type.Argument;
import io.micronaut.messaging.annotation.MessageBody;
import io.micronaut.messaging.annotation.MessageHeader;
import io.micronaut.pulsar.annotation.MessageKey;
import io.micronaut.pulsar.annotation.MessageProperties;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;

import java.util.*;

/**
 * Helper processor class for arguments and headers on Pulsar consumers / processors.
 *
 * @author Haris Secic
 * @since 1.1
 */
@Internal
public final class PulsarArgumentHandler {
    private final LinkedHashMap<String, Argument<?>> methodArguments;
    private final Map<String, Integer> headers;

    public PulsarArgumentHandler(Argument<?>[] methodArguments, final String methodPath) {
        this.methodArguments = processArguments(methodArguments, methodPath);
        this.headers = processSingleHeaders(methodArguments);

        if (hasHeaderList() && hasHeadersMap()) {
            throw new IllegalArgumentException("Cannot have both MessageProperties and individual mappings with " +
                    "MessageHeader on the same method.");
        }
    }

    private static Map<String, Integer> processSingleHeaders(final Argument<?>[] arguments) {
        final Map<String, Integer> headers = new HashMap<>(arguments.length);
        // ensuring order compared to other arguments so no pre-filtering
        for (int i = 0; i < arguments.length; i++) {
            final Argument<?> arg = arguments[i];
            final Optional<AnnotationValue<MessageHeader>> annotation = arg.findAnnotation(MessageHeader.class);
            if (!annotation.isPresent()) {
                continue;
            }
            final String name = annotation.get().stringValue().orElse(arg.getName());
            headers.put(name, i);
        }

        return Collections.unmodifiableMap(headers);
    }

    private static LinkedHashMap<String, Argument<?>> processArguments(final Argument<?>[] arguments, final String methodPath) {
        final LinkedHashMap<String, Argument<?>> ordered = new LinkedHashMap<>(arguments.length);
        if (arguments.length == 1) {
            ordered.put("body", arguments[0]);
            return ordered;
        }

        for (Argument<?> nonHeaderArg : arguments) {
            if (nonHeaderArg.isAnnotationPresent(MessageBody.class)) {
                if (ordered.containsKey("body")) {
                    throw new IllegalArgumentException("Only 1 argument can be mapped as MessageBody on " + methodPath + ".");
                }
                ordered.put("body", nonHeaderArg);
            } else if (nonHeaderArg.isAnnotationPresent(MessageKey.class)) {
                if (ordered.containsKey("key")) {
                    throw new IllegalArgumentException("Only 1 argument can be mapped as MessageKey on " + methodPath + ".");
                }
                ordered.put("key", nonHeaderArg);
            } else if (nonHeaderArg.isAnnotationPresent(MessageProperties.class)) {
                if (ordered.containsKey("headers")) {
                    throw new IllegalArgumentException("Only 1 argument can be mapped as MessageProperties on " + methodPath + ".");
                }
                ordered.put(nonHeaderArg.getName(), nonHeaderArg);
            } else if (Consumer.class.isAssignableFrom(nonHeaderArg.getType())) {
                if (ordered.containsKey("consumer")) {
                    throw new IllegalArgumentException("Only 1 argument can be of type Consumer on " + methodPath + ".");
                }
                ordered.put("consumer", nonHeaderArg);
            } else if (!nonHeaderArg.isAnnotationPresent(MessageHeader.class)) {
                throw new IllegalArgumentException("Argument must be annotated with MessageBody, MessageKey, " +
                        "or MessageProperties when more than 1 are present for " + methodPath);
            }
        }

        if (ordered.containsKey("headers")) {
            final Argument<?> map = ordered.get("headers");
            if (!Map.class.isAssignableFrom(map.getType())) {
                throw new IllegalArgumentException("Argument annotated with @MessageProperties must be a map");
            }
            if (!Arrays.stream(map.getTypeParameters()).allMatch(x -> String.class.isAssignableFrom(x.getType()))) {
                throw new IllegalArgumentException("MessageProperties map can only contain string keys and values");
            }
        }

        if (!ordered.containsKey("body")) {
            throw new IllegalArgumentException("Annotation io.micronaut.messaging.annotation.MessageBody" +
                    " must be present on a single parameter when more than 1 parameter is used with the consumer.");
        }

        return ordered;
    }

    public Argument<?> getBodyArgument() {
        return methodArguments.get("body");
    }

    public boolean isMessageWrapper() {
        return Message.class.isAssignableFrom(methodArguments.get("body").getType());
    }

    @Nullable
    public Argument<?> getKeyArgument() {
        return methodArguments.get("key");
    }

    public boolean hasHeaderList() {
        return !headers.isEmpty();
    }

    public boolean hasHeadersMap() {
        return methodArguments.containsKey("headers");
    }

    public Map<String, Integer> argumentOrder() {
        final Map<String, Integer> order = new HashMap<>(methodArguments.size());
        int i = 0;
        for (String key : methodArguments.keySet()) {
            order.put(key, i);
            i++;
        }
        return Collections.unmodifiableMap(order);
    }

    public Map<String, Integer> headersOrder() {
        return headers;
    }

    public int size() {
        return argumentOrder().size() + headers.size();
    }
}
