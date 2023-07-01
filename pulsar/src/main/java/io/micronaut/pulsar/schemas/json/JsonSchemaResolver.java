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
package io.micronaut.pulsar.schemas.json;

import io.micronaut.json.JsonMapper;
import io.micronaut.pulsar.schemas.SchemaResolver;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.pulsar.client.api.Schema;

/**
 * JSON schema resolver.
 *
 * @author Haris Secic
 * @since 1.1.0
 */
@Singleton
@Named(SchemaResolver.JSON_SCHEMA_NAME)
public class JsonSchemaResolver implements SchemaResolver {

    private final JsonMapper mapper;

    public JsonSchemaResolver(final JsonMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public <T> Schema<T> forArgument(Class<T> pojo) {
        return JsonSchema.of(pojo, mapper);
    }
}
