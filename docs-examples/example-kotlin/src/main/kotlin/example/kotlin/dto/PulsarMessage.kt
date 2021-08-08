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
package example.kotlin.dto

import io.micronaut.core.annotation.Introspected
import io.micronaut.core.annotation.TypeHint
import org.apache.pulsar.client.api.MessageId

/**
 * Simple message data structure.
 */
@Introspected
@TypeHint
data class PulsarMessage(val sent: String, val message: String) {

    /**
     * @return string representation of this message for using with pulsar
     */
    fun toMessage(id: MessageId): String {
        return "$id Message $message sent on $sent"
    }
}