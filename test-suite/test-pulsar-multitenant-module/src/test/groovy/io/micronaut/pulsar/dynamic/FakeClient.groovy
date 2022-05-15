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
package io.micronaut.pulsar.dynamic

import io.micronaut.context.annotation.Requires
import io.micronaut.http.HttpHeaders
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Get
import io.micronaut.http.annotation.Header
import io.micronaut.http.annotation.Post
import io.micronaut.http.client.annotation.Client
import org.apache.pulsar.client.api.Message
import reactor.core.publisher.Mono

@Requires(property = 'spec.name', value = 'DynamicTenantTopicSpec')
@Client('/')
@Header(name = HttpHeaders.USER_AGENT, value = "Micronaut HTTP Client")
interface FakeClient {
    @Post('/messages')
    Mono<String> sendMessage(@Header String tenantId, @Body String message);

    @Get('/messages')
    Mono<MessageResponse> getNextMessage(@Header String tenantId);
}