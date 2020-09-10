package io.micronaut.pulsar;

import org.apache.pulsar.client.api.Producer;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Set;

/**
 * A registry of managed {@link Producer} instances key by id and type.
 *
 * @author Haris Secic
 * @since 1.0
 */
public interface PulsarProducerRegistry {

    /**
     * Get all managed producers
     * @return List of managed producers.
     */
    Map<String, Producer<?>> getProducers();

    /**
     * Get single managed producer by it's name.
     * @param id
     * @return
     */
    Producer<?> getProducer(@Nonnull String id);

    /**
     * Get all managed producer identifiers.
     * @return List of producer names representing their identifiers in registry.
     */
    Set<String> getProducerIds();
}
