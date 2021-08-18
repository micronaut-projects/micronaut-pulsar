package io.kotest.provided

import io.kotest.core.config.AbstractProjectConfig
import io.micronaut.test.extensions.kotest.MicronautKotestExtension
import kotlinexample.PulsarWrapper

object ProjectConfig : AbstractProjectConfig() {
    override fun listeners() = listOf(MicronautKotestExtension)
    override fun extensions() = listOf(MicronautKotestExtension)

    override fun beforeAll() {
        PulsarWrapper.start()
    }

    override fun afterAll() {
        PulsarWrapper.stop()
    }
}
