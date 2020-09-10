package io.micronaut.pulsar.annotation;

public enum MessageSchema {
    BYTES,
    BYTEBUFFER,
    INT8,
    INT16,
    INT32,
    INT64,
    BOOL,
    FLOAT,
    DOUBLE,
    DATE,
    TIME,
    TIMESTAMP,
    STRING,
    JSON,
    AVRO,
    PROTOBUF,
    KEY_VALUE
}
