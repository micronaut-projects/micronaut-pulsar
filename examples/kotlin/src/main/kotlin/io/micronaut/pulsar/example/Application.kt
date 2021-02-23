package io.micronaut.pulsar.example

import io.micronaut.runtime.Micronaut.*
fun main(args: Array<String>) {
	build()
	    .args(*args)
		.packages("io.micronaut.pulsar.example")
		.start()
}

