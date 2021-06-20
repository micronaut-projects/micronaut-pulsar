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
import io.micronaut.pulsar.MessageSchema;
import io.micronaut.pulsar.schemas.JsonSchema;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.*;

import javax.inject.Singleton;

/**
 * Message type resolver for Pulsar schema. Uses customised Schema for JSON; otherwise falls back to the ones
 * from Apache Pulsar Java library.
 * @author Haris Secic
 * @since 1.0
 */
@Singleton
public class SchemaResolver {

    private final ObjectMapper objectMapper;

    public SchemaResolver(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * Resolve which schema to use.
     * @param topicAnnotation either producer or consumer annotation
     * @param messageBodyType type of message body used with Pulsar topic
     * @return new Schema
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Schema<?> decideSchema(final AnnotationValue<?> topicAnnotation, Class<?> messageBodyType) {

        MessageSchema schema = topicAnnotation.getRequiredValue("schema", MessageSchema.class);
        if (MessageSchema.BYTES == schema && byte[].class != messageBodyType) {
            if (String.class == messageBodyType) {
                return new StringSchema();
            }
            return JsonSchema.of(messageBodyType, objectMapper); //default to JSON for now
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
                return JsonSchema.of(messageBodyType, objectMapper);
            case AVRO:
                return AvroSchema.of(new SchemaDefinitionBuilderImpl().withPojo(messageBodyType).build());
            case PROTOBUF:
                throw new UnsupportedOperationException("Missing implementation for Protobuf schema message");
            case KEY_VALUE:
                throw new UnsupportedOperationException("Missing implementation for KEY_VALUE schema message");
            default:
                throw new IllegalStateException("Unexpected value: " + schema);
        }
    }
}
