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
package example.web;

import io.micronaut.websocket.WebSocketBroadcaster;
import io.micronaut.websocket.WebSocketSession;
import io.micronaut.websocket.annotation.OnClose;
import io.micronaut.websocket.annotation.OnMessage;
import io.micronaut.websocket.annotation.OnOpen;
import io.micronaut.websocket.annotation.ServerWebSocket;

/**
 * TODO javadoc.
 */
@ServerWebSocket("/ws/{tenant}/{namespace}/{topic}")
public class PulsarMessageShareSocket {

    private final WebSocketBroadcaster broadcaster;

    public PulsarMessageShareSocket(WebSocketBroadcaster broadcaster) {
        this.broadcaster = broadcaster;
    }

    /**
     * @param tenant TODO
     * @param namespace TODO
     * @param topic TODO
     * @param session TODO
     */
    @OnOpen
    public void onOpen(String tenant, String namespace, String topic, WebSocketSession session) {
        String report = String.format("A user %s has joined %s/%s/%s", session.getId(), tenant, namespace, topic);
        broadcaster.broadcastAsync(report, this::isReport);
    }

    /**
     * @param tenant TODO
     * @param namespace TODO
     * @param topic TODO
     * @param message TODO
     * @param ws TODO
     */
    @OnMessage
    public void onMessage(String tenant, String namespace, String topic, String message, WebSocketSession ws) {
        String id = "PULSAR";
        if (null != ws) {
            id = ws.getId();
        }
        String report = String.format("A message has been received on %s/%s/%s from %s", tenant, namespace, topic, id);
        broadcaster.broadcastAsync(report, this::isReport);
    }

    /**
     * @param session TODO
     */
    @OnClose
    public void onClose(WebSocketSession session) {
        String message = String.format("A user with session ID %s has Disconnected!", session.getId());
        broadcaster.broadcastAsync(message, this::isReport);
    }

    private Boolean isReport(WebSocketSession wss) {
        String tenant = wss.getUriVariables().get("tenant", String.class, null);
        if (!tenant.equalsIgnoreCase("private")) {
            return false;
        }
        String namespace = wss.getUriVariables().get("namespace", String.class, null);
        if (!namespace.equalsIgnoreCase("reports")) {
            return false;
        }
        return wss.getUriVariables().get("topic", String.class, "").equalsIgnoreCase("messages");
    }
}
