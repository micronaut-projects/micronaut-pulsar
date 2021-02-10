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
package io.micronaut.pulsar;

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
    BYTES,

    /**
     * Effectively a `BYTES` schema.
     */
    BYTEBUFFER,

    /**
     * A 8-bit signed integer.
     */
    INT8,

    /**
     * A 16-bit signed integer.
     */
    INT16,

    /**
     * A 32-bit signed integer.
     */
    INT32,

    /**
     * A 64-bit signed integer.
     */
    INT64,

    /**
     * A binary value.
     */
    BOOL,

    /**
     * A single precision (32-bit) IEEE 754 floating-point number.
     */
    FLOAT,

    /**
     * A double-precision (64-bit) IEEE 754 floating-point number.
     */
    DOUBLE,

    /**
     * A schema for `java.util.Date` or `java.sql.Date`.
     */
    DATE,

    /**
     * A schema for `java.sql.Time`.
     */
    TIME,

    /**
     * A schema for `java.sql.Timestamp`.
     */
    TIMESTAMP,

    /**
     * A Unicode character sequence.
     */
    STRING,

    /**
     * A schema for JSON data.
     */
    JSON,

    /**
     * An Apache Avro schema.
     */
    AVRO,

    /**
     * A schema for Protocol Buffer generated messages.
     */
    PROTOBUF,

    /**
     * Key/Value pair schema.
     */
    KEY_VALUE
}
