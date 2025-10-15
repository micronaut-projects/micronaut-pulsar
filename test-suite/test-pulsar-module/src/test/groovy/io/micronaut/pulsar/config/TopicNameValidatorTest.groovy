package io.micronaut.pulsar.config

import spock.lang.Specification

import java.util.regex.Pattern

class TopicNameValidatorTest extends Specification {

    void "should allow valid topic names"() {
        given:
        def compiled = Pattern.compile(AbstractPulsarConfiguration.TOPIC_NAME_VALIDATOR)
        def validTopics = ["tenant/namespace/topic","tenant/namespace/topic.with.dots","tenant/namespace/topic-with-dashes", "tenant/namespace/Topic-combo.doTs-and.Dashes"]

        expect:
        validTopics.every {it.matches(compiled)}
    }

    void "should prevent invalid topic names"() {
        given:
        def compiled = Pattern.compile(AbstractPulsarConfiguration.TOPIC_NAME_VALIDATOR)
        def validTopics = ["tenant/namespace/topic*","tenant/namespace/1+2"]

        expect:
        validTopics.every {!it.matches(compiled)}
    }
}
