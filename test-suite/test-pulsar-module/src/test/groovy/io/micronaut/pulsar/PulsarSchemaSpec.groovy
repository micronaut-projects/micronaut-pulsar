package io.micronaut.pulsar

import io.micronaut.context.annotation.Requires
import io.micronaut.messaging.annotation.MessageBody
import io.micronaut.pulsar.annotation.PulsarConsumer
import io.micronaut.pulsar.annotation.PulsarProducer
import io.micronaut.pulsar.annotation.PulsarProducerClient
import io.micronaut.pulsar.annotation.PulsarSubscription
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.MessageId
import spock.lang.Stepwise
import spock.util.concurrent.BlockingVariables

@Stepwise
class PulsarSchemaSpec extends PulsarAwareTest {

    public static final String PULSAR_JSON_TOPIC = "persistent://public/default/json"
    public static final String PULSAR_PROTOBUF_TOPIC = "persistent://public/default/protobuf"

    void "test send receive json"() {
        given:
        BlockingVariables vars = new BlockingVariables(65)

        when:
        PulsarSchemaTestListener topicTester = context.getBean(PulsarSchemaTestListener.class)
        topicTester.blockers = vars
        PulsarSchemaTestProducer producer = context.getBean(PulsarSchemaTestProducer.class)
        //simple consumer with topic list and blocking
        JsonMessage message = new JsonMessage(text: "Text value", number: 2)
        MessageId jsonId = producer.sendJson(message)

        then:
        null != jsonId
        jsonId == vars.getProperty("json_message_id")
        message.properties == vars.getProperty("json_value").properties
    }

    void "test send receive protobuf"() {
        given:
        BlockingVariables vars = new BlockingVariables(65)

        when:
        PulsarSchemaTestListener topicTester = context.getBean(PulsarSchemaTestListener.class)
        topicTester.blockers = vars
        PulsarSchemaTestProducer producer = context.getBean(PulsarSchemaTestProducer.class)
        //simple consumer with topic list and blocking
        ProtoMessages.ProtoMessage message = ProtoMessages.ProtoMessage.newBuilder()
                .setMessage("Text value")
                .setNumber(2)
                .build()
        MessageId protobufId = producer.sendProto(message)

        then:
        null != protobufId
        protobufId == vars.getProperty("proto_message_id")
        message == vars.getProperty("proto_value")
    }

    @Requires(property = 'spec.name', value = 'PulsarSchemaSpec')
    @PulsarProducerClient
    static interface PulsarSchemaTestProducer {
        @PulsarProducer(schema = MessageSchema.JSON, topic = PulsarSchemaSpec.PULSAR_JSON_TOPIC)
        MessageId sendJson(JsonMessage message)

        @PulsarProducer(schema = MessageSchema.PROTOBUF, topic = PulsarSchemaSpec.PULSAR_PROTOBUF_TOPIC)
        MessageId sendProto(ProtoMessages.ProtoMessage message)
    }

    @Requires(property = 'spec.name', value = 'PulsarSchemaSpec')
    @PulsarSubscription(subscriptionName = "subscriber-schema")
    static class PulsarSchemaTestListener {
        BlockingVariables blockers

        @PulsarConsumer(
                topic = PulsarSchemaSpec.PULSAR_JSON_TOPIC,
                consumerName = 'json-topic-consumer',
                subscribeAsync = false)
        void jsonListener(Message<JsonMessage> message) {
            if (null == blockers) {
                return
            }
            blockers.setProperty("json_message_id", message.messageId)
            blockers.setProperty("json_value", message.getValue())
        }

        @PulsarConsumer(
                topic = PulsarSchemaSpec.PULSAR_PROTOBUF_TOPIC,
                consumerName = 'protobuf-topic-consumer',
                schema = MessageSchema.PROTOBUF,
                subscribeAsync = false)
        void protobufListener(@MessageBody Message<ProtoMessages.ProtoMessage> message) {
            if (null == blockers) {
                return
            }
            blockers.setProperty("proto_message_id", message.messageId)
            blockers.setProperty("proto_value", message.getValue())
        }
    }

    static class JsonMessage {
        String text
        Integer number
    }
}
