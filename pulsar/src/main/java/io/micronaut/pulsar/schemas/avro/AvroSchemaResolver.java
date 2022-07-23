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
package io.micronaut.pulsar.schemas.avro;

import io.micronaut.pulsar.schemas.SchemaResolver;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.SchemaDefinitionBuilderImpl;

/**
 * AVRO schema resolver.
 *
 * @author Haris Secic
 * @since 1.2.1
 */
@Singleton
@Named(SchemaResolver.AVRO_SCHEMA_NAME)
public final class AvroSchemaResolver implements SchemaResolver {
    @Override
    public <T> Schema<T> forArgument(final Class<T> pojo) {
        return AvroSchema.of(new SchemaDefinitionBuilderImpl<T>()
            .withPojo(pojo)
            .build());
    }
}
