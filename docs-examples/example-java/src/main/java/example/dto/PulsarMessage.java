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
package example.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Simple message data structure.
 */
public class PulsarMessage {

    private final String message;
    private final String sent;

    @JsonCreator
    public PulsarMessage(@JsonProperty("sent") String sent,
                         @JsonProperty("message") String message) {
        this.message = message;
        this.sent = sent;
    }

    /**
     * @return message
     */
    public String getMessage() {
        return message;
    }

    /**
     * @return sent date time as string
     */
    public String getSent() {
        return sent;
    }

    /**
     * @return string representation of this message for using with pulsar
     */
    public String toMessage() {
        return String.format("Message %s sent on %s", message, sent);
    }
}
