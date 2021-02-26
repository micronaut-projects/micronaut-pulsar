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
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.schema.SchemaReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * JSON Schema Reader to allow using {@link ObjectMapper} from Micronaut instead of shaded one in Pulsar library.
 *
 * @author Haris Secic
 * @since 1.0
 */
public class JacksonJsonReader<T> implements SchemaReader<T> {
    private final Class<T> pojo;
    private final ObjectMapper objectMapper;
    private static final Logger LOG = LoggerFactory.getLogger(JacksonJsonReader.class);

    public JacksonJsonReader(ObjectMapper objectMapper, Class<T> pojo) {
        this.pojo = pojo;
        this.objectMapper = objectMapper;
    }

    public T read(byte[] bytes, int offset, int length) {
        try {
            return this.objectMapper.readValue(bytes, offset, length, this.pojo);
        } catch (IOException var5) {
            throw new SchemaSerializationException(var5);
        }
    }

    public T read(InputStream inputStream) {
        T var2;
        try {
            var2 = this.objectMapper.readValue(inputStream, this.pojo);
            try {
                inputStream.close();
            } catch (IOException var10) {
                LOG.error("JsonReader close inputStream close error", var10);
            }
        } catch (IOException var11) {
            try {
                inputStream.close();
            } catch (IOException var10) {
                LOG.error("JsonReader close inputStream close error", var10);
            }
            throw new SchemaSerializationException(var11);
        }
        // avoiding finally as there's no guarantee for how long it takes to close the stream
        // can't use try with resource since resource is passed
        return var2;
    }
}
