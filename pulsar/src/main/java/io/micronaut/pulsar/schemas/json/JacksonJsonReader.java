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
package io.micronaut.pulsar.schemas.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.core.type.Argument;
import io.micronaut.jackson.databind.JacksonDatabindMapper;
import io.micronaut.json.JsonMapper;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.schema.SchemaReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * JSON Schema Reader to allow using {@link ObjectMapper} from Micronaut instead of shaded one in Pulsar library.
 *
 * @param <T> POJO type to process.
 * @author Haris Secic
 * @since 1.0
 */
public final class JacksonJsonReader<T> implements SchemaReader<T> {
    private static final Logger LOG = LoggerFactory.getLogger(JacksonJsonReader.class);

    private final Class<T> pojo;
    private final JsonMapper jsonMapper;

    /**
     * @param objectMapper The jackson object mapper to use for reading
     * @param pojo         The pojo type to read
     * @deprecated Use {@link #JacksonJsonReader(JsonMapper, Class)} instead
     */
    @Deprecated
    public JacksonJsonReader(ObjectMapper objectMapper, Class<T> pojo) {
        this(new JacksonDatabindMapper(objectMapper), pojo);
    }

    /**
     * @param jsonMapper The json mapper to use for reading
     * @param pojo       The pojo type to read
     * @since 1.1.0
     */
    public JacksonJsonReader(JsonMapper jsonMapper, Class<T> pojo) {
        this.pojo = pojo;
        this.jsonMapper = jsonMapper;
    }

    public T read(byte[] bytes, int offset, int length) {
        try {
            return this.jsonMapper.readValue(new ByteArrayInputStream(bytes, offset, length), Argument.of(this.pojo));
        } catch (IOException ex) {
            throw new SchemaSerializationException(ex);
        }
    }

    public T read(InputStream inputStream) {
        final T value;
        try {
            value = this.jsonMapper.readValue(inputStream, Argument.of(this.pojo));
            try {
                inputStream.close();
            } catch (IOException closeException) {
                LOG.error("JsonReader close inputStream close error", closeException);
            }
        } catch (IOException ioException) {
            try {
                inputStream.close();
            } catch (IOException closeException) {
                LOG.error("JsonReader close inputStream close error", closeException);
            }
            throw new SchemaSerializationException(ioException);
        }
        // avoiding finally as there's no guarantee for how long it takes to close the stream
        // can't use try with resource since resource is passed
        return value;
    }
}
