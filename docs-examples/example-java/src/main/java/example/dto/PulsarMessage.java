package example.dto;

import java.time.OffsetDateTime;

public class PulsarMessage {

    private final String message;
    private final OffsetDateTime sent;

    public PulsarMessage(String message, OffsetDateTime sent) {
        this.message = message;
        this.sent = sent;
    }

    public String getMessage() {
        return message;
    }

    public OffsetDateTime getSent() {
        return sent;
    }
}
