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
package io.micronaut.pulsar;

import io.micronaut.core.annotation.Nullable;
import io.micronaut.pulsar.schemas.SchemaResolver;

/**
 * Supported schema types.
 *
 * @author Haris Secic
 * @since 1.0
 */
public enum MessageSchema {

    /**
     * A sequence of 8-bit unsigned bytes.
     */
    BYTES(null),

    /**
     * Effectively a `BYTES` schema.
     */
    BYTEBUFFER(null),

    /**
     * Effectively a `BYTES` schema of a single byte.
     */
    INT8(null),

    /**
     * A 16-bit signed integer.
     */
    INT16(null),

    /**
     * A 32-bit signed integer.
     */
    INT32(null),

    /**
     * A 64-bit signed integer.
     */
    INT64(null),

    /**
     * A binary value.
     */
    BOOL(null),

    /**
     * A single precision (32-bit) IEEE 754 floating-point number.
     */
    FLOAT(null),

    /**
     * A double-precision (64-bit) IEEE 754 floating-point number.
     */
    DOUBLE(null),

    /**
     * A schema for `java.util.Date` or `java.sql.Date`.
     */
    DATE(null),

    /**
     * A schema for `java.sql.Time`.
     */
    TIME(null),

    /**
     * A schema for `java.sql.Timestamp`.
     */
    TIMESTAMP(null),

    /**
     * A Unicode character sequence.
     */
    STRING(null),

    /**
     * A schema for JSON data.
     */
    JSON(SchemaResolver.JSON_SCHEMA_NAME),

    /**
     * An Apache Avro schema.
     */
    AVRO(SchemaResolver.AVRO_SCHEMA_NAME),

    /**
     * A schema for Protocol Buffer generated messages.
     */
    PROTOBUF(SchemaResolver.PROTOBUF_SCHEMA_NAME);

    final String schemaResolverName;

    MessageSchema(@Nullable String namedBean) {
        this.schemaResolverName = namedBean;
    }

    @Nullable
    public String getSchemaResolverName() {
        return schemaResolverName;
    }
}
