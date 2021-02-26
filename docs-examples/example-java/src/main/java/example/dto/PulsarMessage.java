package example.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class PulsarMessage {

    private final String message;
    private final String sent;

    @JsonCreator
    public PulsarMessage(@JsonProperty("sent") String sent, @JsonProperty("message") String message) {
        this.message = message;
        this.sent = sent;
    }

    public String getMessage() {
        return message;
    }

    public String getSent() {
        return sent;
    }

    public String toMessage() {
        return String.format("Message %s sent on %s", message, sent);
    }
}
