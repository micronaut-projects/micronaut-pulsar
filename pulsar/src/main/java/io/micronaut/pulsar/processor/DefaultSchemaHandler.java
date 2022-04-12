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

import io.micronaut.context.BeanContext;
import io.micronaut.core.annotation.AnnotationValue;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.type.Argument;
import io.micronaut.inject.qualifiers.Qualifiers;
import io.micronaut.messaging.exceptions.MessageListenerException;
import io.micronaut.pulsar.MessageSchema;
import io.micronaut.pulsar.schemas.SchemaResolver;
import jakarta.inject.Singleton;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.*;
import org.apache.pulsar.common.schema.KeyValueEncodingType;

/**
 * Message type resolver for Pulsar schema. Simplifies resolving Java types to Pulsar schemas by using requested
 * transmission type via annotations and injecting proper resolvers in place.
 *
 * @author Haris Secic
 * @since 1.1.0
 */
@Singleton
public class DefaultSchemaHandler {

    private final BeanContext context;

    /**
     * @param context BeanContext for fetching extra resolvers
     * @since 1.1.0
     */
    public DefaultSchemaHandler(final BeanContext context) {
        this.context = context;
    }

    /**
     * Resolve which schema to use for ser/der.
     *
     * @param body            argument that represents message body
     * @param key             if message is of type key-value a key should be passed; otherwise use null
     * @param topicAnnotation annotation corresponding to one of the Pulsar annotations: consumer, reader, producer.
     * @param target          name of the method, field, or other that represent where the error might be in case of it.
     * @return new Schema
     */
    public Schema<?> decideSchema(final Argument<?> body,
                                  @Nullable final Argument<?> key,
                                  final AnnotationValue<?> topicAnnotation,
                                  final String target) {
        final boolean isKeyValue = key != null;
        final MessageSchema schema = topicAnnotation.getRequiredValue("schema", MessageSchema.class);
        if (!isKeyValue) {
            return resolve(schema, body, target);
        }
        final KeyValueEncodingType type = topicAnnotation.getRequiredValue(KeyValueEncodingType.class);
        final Schema<?> bodyType = resolve(schema, body, target);
        final Schema<?> keyType;
        if (type == KeyValueEncodingType.INLINE) {
            keyType = resolve(schema, key, target);
        } else {
            keyType = resolve(topicAnnotation.getRequiredValue("keySchema", MessageSchema.class), key, target);
        }
        return KeyValueSchemaImpl.of(bodyType, keyType, type);
    }

    private Schema<?> resolve(final MessageSchema schema, final Argument<?> argument, final String target) {
        final Class<?> type = bodyType(argument);

        if (MessageSchema.BYTES == schema && byte[].class != type) {
            if (String.class == type) {
                return StringSchema.utf8();
            }
            return context.getBean(SchemaResolver.class, Qualifiers.byName(MessageSchema.JSON.getSchemaResolverName()))
                    .forArgument(type); //default to JSON for now
        }

        switch (schema) {
            case BYTES:
                return BytesSchema.of();
            case BYTEBUFFER:
                return ByteBufferSchema.of();
            case INT8:
                return ByteSchema.of();
            case INT16:
                return ShortSchema.of();
            case INT32:
                return IntSchema.of();
            case INT64:
                return LongSchema.of();
            case BOOL:
                return BooleanSchema.of();
            case FLOAT:
                return FloatSchema.of();
            case DOUBLE:
                return DoubleSchema.of();
            case DATE:
                return DateSchema.of();
            case TIME:
                return TimeSchema.of();
            case TIMESTAMP:
                return TimestampSchema.of();
            case STRING:
                return StringSchema.utf8();
            case AVRO:
                return AvroSchema.of(new SchemaDefinitionBuilderImpl<>().withPojo(type).build());
            default:
                try {
                    return context.getBean(SchemaResolver.class, Qualifiers.byName(schema.getSchemaResolverName()))
                            .forArgument(type);
                } catch (final MessageListenerException typeHandlingException) {
                    throw new MessageListenerException(typeHandlingException.getMessage() +
                            " (parameter: " + target + ")");
                }
        }
    }

    public static Class<?> bodyType(final Argument<?> body) {
        if (Message.class.isAssignableFrom(body.getType())) {
            return body.getTypeParameters()[0].getType();
        }
        return body.getType();
    }
}
