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
package io.micronaut.pulsar.schemas;

import org.apache.pulsar.client.api.Schema;

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

/***
 * SchemaResolver represents basic bean that will handle serde operations for a given type. Beans should be named and
 * each should represent schema which Apache Pulsar should use as transport type. For example JSON is wrapped in AVRO
 * and Protobuf is natively support but thus 2 SchemaResolvers should exist. To avoid dependency checking default type
 * names are listed here.
 *
 * Note: Primitive types are passed down to official Pulsar Java library thus no SchemaResolver names are listed here
 * for them.
 */
public interface SchemaResolver {
    String JSON_SCHEMA_NAME = "JSON_SCHEMA_RESOLVER";
    String PROTOBUF_SCHEMA_NAME = "PROTOBUF_SCHEMA_RESOLVER";
    String AVRO_SCHEMA_NAME = "AVRO_SCHEMA_RESOLVER";

    <T> Schema<T> forArgument(Class<T> pojo);
}
