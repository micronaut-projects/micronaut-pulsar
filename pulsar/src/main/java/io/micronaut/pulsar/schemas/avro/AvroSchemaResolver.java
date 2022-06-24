package io.micronaut.pulsar.schemas.avro;

import io.micronaut.pulsar.schemas.SchemaResolver;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.SchemaDefinitionBuilderImpl;

/**
 * AVRO schema resolver.
 *
 * @author Haris Secic
 * @since 1.2.1
 */
@Singleton
@Named(SchemaResolver.AVRO_SCHEMA_NAME)
public final class AvroSchemaResolver implements SchemaResolver {
    @Override
    public <T> Schema<T> forArgument(final Class<T> pojo) {
        return AvroSchema.of(new SchemaDefinitionBuilderImpl<T>()
            .withPojo(pojo)
            .build());
    }
}
