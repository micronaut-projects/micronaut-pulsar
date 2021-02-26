package example.web;

import io.micronaut.websocket.WebSocketBroadcaster;
import io.micronaut.websocket.WebSocketSession;
import io.micronaut.websocket.annotation.OnClose;
import io.micronaut.websocket.annotation.OnMessage;
import io.micronaut.websocket.annotation.OnOpen;
import io.micronaut.websocket.annotation.ServerWebSocket;

@ServerWebSocket("/ws/{tenant}/{namespace}/{topic}")
public class PulsarMessageShareSocket {
    private final WebSocketBroadcaster broadcaster;

    public PulsarMessageShareSocket(WebSocketBroadcaster broadcaster) {
        this.broadcaster = broadcaster;
    }

    @OnOpen
    public void onOpen(String tenant, String namespace, String topic, WebSocketSession session) {
        String report = String.format("A user %s has joined %s/%s/%s", session.getId(), tenant, namespace, topic);
        broadcaster.broadcastAsync(report, this::isReport);
    }

    @OnMessage
    public void onMessage(String tenant, String namespace, String topic, String message, WebSocketSession ws) {
        String id = "PULSAR";
        if (null != ws) id = ws.getId();
        String report = String.format("A message has been received on %s/%s/%s from %s", tenant, namespace, topic, id);
        broadcaster.broadcastAsync(report, this::isReport);
    }

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
