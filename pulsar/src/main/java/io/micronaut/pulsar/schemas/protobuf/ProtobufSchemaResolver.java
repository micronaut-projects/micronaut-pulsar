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
package io.micronaut.pulsar.schemas.protobuf;

import com.google.protobuf.Message;
import io.micronaut.messaging.exceptions.MessageListenerException;
import io.micronaut.protobuf.codec.ProtobufferCodec;
import io.micronaut.pulsar.schemas.ProtobufSchema;
import io.micronaut.pulsar.schemas.SchemaResolver;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.pulsar.client.api.Schema;

/**
 * Protobuf schema resolver.
 *
 * @author Haris Secic
 * @since 1.1.0
 */
@Singleton
@Named(SchemaResolver.PROTOBUF_SCHEMA_NAME)
public class ProtobufSchemaResolver implements SchemaResolver {

    private final ProtobufferCodec codec;

    public ProtobufSchemaResolver(final ProtobufferCodec codec) {
        this.codec = codec;
    }

    @Override
    public Schema<?> forArgument(Class<?> pojo) {
        if (!Message.class.isAssignableFrom(pojo)) {
            throw new MessageListenerException("Protocol buffers (native) are only supported for types that implement com.google.protobuf.Message");
        }
        return ProtobufSchema.of(pojo, codec);
    }
}
