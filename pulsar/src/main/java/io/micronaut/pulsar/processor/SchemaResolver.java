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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.core.annotation.AnnotationValue;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.beans.BeanWrapper;
import io.micronaut.core.naming.Named;
import io.micronaut.core.type.Argument;
import io.micronaut.jackson.databind.JacksonDatabindMapper;
import io.micronaut.json.JsonMapper;
import io.micronaut.pulsar.MessageSchema;
import io.micronaut.pulsar.schemas.JsonSchema;
import jakarta.inject.Singleton;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.*;
import org.apache.pulsar.common.schema.KeyValueEncodingType;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Message type resolver for Pulsar schema. Uses customised Schema for JSON; otherwise falls back to the ones
 * from Apache Pulsar Java library.
 *
 * @author Haris Secic
 * @since 1.0
 */
@Singleton
public class SchemaResolver {

    private final JsonMapper jsonMapper;

    /**
     * @param objectMapper The jackson object mapper
     * @deprecated Use {@link #SchemaResolver(JsonMapper)} instead
     */
    @Deprecated
    public SchemaResolver(final ObjectMapper objectMapper) {
        this(new JacksonDatabindMapper(objectMapper));
    }

    /**
     * @param jsonMapper The JSON mapper to use
     * @since 1.1.0
     */
    public SchemaResolver(final JsonMapper jsonMapper) {
        this.jsonMapper = jsonMapper;
    }

    /**
     * Resolve which schema to use for ser/der.
     *
     * @param topicAnnotation annotation corresponding to one of the Pulsar annotations: consumer, reader, producer.
     * @param body argument that represents message body
     * @param key if message is of type key-value a key should be passed; otherwise use null
     * @return new Schema
     */
    public Schema<?> decideSchema(final Argument<?> body, @Nullable final Argument<?> key, final AnnotationValue<?> topicAnnotation) {
        final boolean isKeyValue = key != null;
        final Class<?> bodyClass = bodyType(body);
        final MessageSchema schema = topicAnnotation.getRequiredValue("schema", MessageSchema.class);
        if (!isKeyValue) {
            return resolve(schema, bodyClass);
        }
        final KeyValueEncodingType type = topicAnnotation.getRequiredValue(KeyValueEncodingType.class);
        final Schema<?> bodyType = resolve(schema, bodyClass);
        final Schema<?> keyType;
        if (type == KeyValueEncodingType.INLINE) {
            keyType = resolve(schema, key.getType());
        } else {
            keyType = resolve(topicAnnotation.getRequiredValue("keySchema", MessageSchema.class),
                    key.getType());
        }
        return KeyValueSchemaImpl.of(bodyType, keyType, type);
    }

    private Schema<?> resolve(final MessageSchema schema, final Class<?> type) {
        if (MessageSchema.BYTES == schema && byte[].class != type) {
            if (String.class == type) {
                return new StringSchema();
            }
            return JsonSchema.of(type, jsonMapper); //default to JSON for now
        }

        switch (schema) {
            case BYTES:
                return new BytesSchema();
            case BYTEBUFFER:
                return new ByteBufferSchema();
            case INT8:
                return new ByteSchema();
            case INT16:
                return new ShortSchema();
            case INT32:
                return new IntSchema();
            case INT64:
                return new LongSchema();
            case BOOL:
                return new BooleanSchema();
            case FLOAT:
                return new FloatSchema();
            case DOUBLE:
                return new DoubleSchema();
            case DATE:
                return new DateSchema();
            case TIME:
                return new TimeSchema();
            case TIMESTAMP:
                return new TimestampSchema();
            case STRING:
                return new StringSchema();
            case JSON:
                return JsonSchema.of(type, jsonMapper);
            case AVRO:
                return AvroSchema.of(new SchemaDefinitionBuilderImpl<>().withPojo(type).build());
            case PROTOBUF:
                Map<String, String> properties = BeanWrapper.getWrapper(type).getBeanProperties().stream()
                        .collect(Collectors.toMap(Named::getName, x -> x.getType().getName()));
                properties.put("__jsr310ConversionEnabled", "true");
                properties.put("__alwaysAllowNull", "true");
                return ProtobufNativeSchema.ofGenericClass(type, properties);
            default:
                throw new IllegalStateException("Unexpected value: " + schema);
        }
    }

    private static Class<?> bodyType(final Argument<?> body) {
        if (Message.class.isAssignableFrom(body.getType())) {
            return body.getTypeParameters()[0].getType();
        }
        return body.getType();
    }
}
