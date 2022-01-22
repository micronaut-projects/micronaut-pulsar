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
package io.micronaut.pulsar.schemas.protobuf;

import io.micronaut.protobuf.codec.ProtobufferCodec;
import org.apache.pulsar.client.api.schema.SchemaWriter;

/**
 * Protobuf Schema Writer to allow using {@link ProtobufferCodec} from Micronaut.
 *
 * @param <T> POJO type to process.
 * @author Haris Secic
 * @since 1.1.0
 */
public final class ProtobufWriter<T> implements SchemaWriter<T> {

    private final ProtobufferCodec codec;

    public ProtobufWriter(ProtobufferCodec codec) {
        this.codec = codec;
    }

    @Override
    public byte[] write(Object message) {
        return codec.encode(message);
    }

}
