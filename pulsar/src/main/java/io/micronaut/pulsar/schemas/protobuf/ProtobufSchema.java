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

import io.micronaut.core.type.Argument;
import io.micronaut.protobuf.codec.ProtobufferCodec;
import org.apache.avro.Schema;
import org.apache.avro.protobuf.ProtobufData;
import org.apache.pulsar.client.impl.schema.AbstractStructSchema;
import org.apache.pulsar.client.impl.schema.SchemaInfoImpl;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * JSON Schema to allow using {@link ProtobufferCodec} from Micronaut.
 *
 * @param <T> POJO type to send and receive via Pulsar.
 * @author Haris Secic
 * @since 1.0
 */
public final class ProtobufSchema<T> extends AbstractStructSchema<T> {

    private static final Map<Integer, ProtobufSchema<?>> SCHEMAS = new ConcurrentHashMap<>(10);

    public ProtobufSchema(final SchemaInfo schemaInfo, final ProtobufReader<T> reader, final ProtobufWriter<T> writer) {
        super(schemaInfo);
        this.setReader(reader);
        this.setWriter(writer);
    }

    @SuppressWarnings("unchecked")
    public static <T> ProtobufSchema<T> of(final Class<T> type, final ProtobufferCodec codec) {
        return (ProtobufSchema<T>) SCHEMAS.computeIfAbsent(type.hashCode(), x -> {
            final ProtobufWriter<T> writer = new ProtobufWriter<>(codec);
            final ProtobufReader<T> reader = new ProtobufReader<>(codec, Argument.of(type));
            final Schema schema = ProtobufData.get().getSchema(type);
            final SchemaInfo schemaInfo = SchemaInfoImpl.builder()
                    .schema(schema.toString().getBytes(UTF_8))
                    .type(SchemaType.PROTOBUF)
                    .name("")
                    .properties(new HashMap<>())
                    .build();
            return new ProtobufSchema<>(schemaInfo, reader, writer);
        });
    }
}
