# Python Docs Disabled Test Inventory

This file tracks the Python documentation examples under `doc-examples/example-python`
that are present but disabled, or that deviate from the Java example because the direct port does
not compile or does not behave like the Java example yet. It is the bug-fixing task list for
the Python compiler (`micronaut-inject-python` / `micronaut-context-python`); every row references a
`TODO(python)` comment in the sources or a workaround described below.

The Python examples are compiled by every build and their tests run with
`./gradlew pythonCheck -Ppython-ci` (the "Python CI" GitHub workflow), which needs a container
runtime for the Pulsar test container.

## Reconciliation

- Last generated active `@Disabled` count: 1.
- Last generated command: `rg -n "@Disabled\(" doc-examples/example-python/src/test/python`.
- Last full-suite command: `./gradlew :micronaut-doc-examples:micronaut-example-python:test -Ppython-ci`.
- Last full-suite result: build successful, 4 tests executed, 1 skipped (`test_consumer`, see below), 0 failures.

## Migration Rules

- Do not define local copies of Micronaut annotation helpers or custom annotation shims in docs snippets.
  The Micronaut and Pulsar annotations are imported from their Java packages (`micronaut.pulsar.annotation`,
  `jakarta.inject`, `org.apache.pulsar.client.api`).
- `@PulsarProducerClient` interfaces are abstract classes (`ABC`) whose `@PulsarProducer` methods are
  `@abstractmethod`s with `...` bodies; `@PulsarSubscription` beans are plain classes; `@PulsarReader` fields are
  injected class attributes (`reader: Annotated[Reader[str], Inject, PulsarReader(...)]`).
- Methods are snake_case (`send_blocking`, `message_printer`, `read_next`); Java classes are imported
  (`from java.util.concurrent import CompletableFuture`, `from org.apache.pulsar.client.api import MessageId`).
- The example sources live in `src/main/python` (the guide uses `source="main"` snippets) and the test in
  `src/test/python`; both roots are merged into one compilation by the `mergePythonSources` task of the build
  file, see below.
- The test class is a `@MicronautTest(environments=["pulsar"])` with injected example beans and the
  `PulsarClient`; the `pulsar.service-url` of the Pulsar test container is supplied by the Java
  `io.micronaut.pulsar.shared.PulsarTestConfigurer` (`@ContextConfigurer`) of the
  `test-suite:test-pulsar-shared-module` project, shared with the Java, Kotlin and Groovy example tests.

## Active `@Disabled` Tests

| Test | Reason |
| --- | --- |
| `example.PulsarExamplesTest.test_consumer` | `@PulsarConsumer` is meta-annotated with `@MessageListener`, a `@Bean` stereotype, and the Python compiler treats every method carrying a declared `@Bean` stereotype as a `@Bean` factory method, whether or not the class is a `@Factory` (`Factory methods declared with @Bean must specify a return type`). The `@PulsarConsumer` decorator of `example.ConsumerProducer.message_printer` is therefore commented out (`TODO(python)`), no consumer is subscribed and the test expecting the consumed message to be reported is disabled. |

## Commented Unsupported Snippet Ports

| Target | Reason |
| --- | --- |
| `example.ConsumerProducer` (`@PulsarConsumer` decorator of `message_printer`) | See `test_consumer` above; the guide carries a `[.lang-python]` warning. |

## Workarounds Kept In Snippets

| Target | Reason |
| --- | --- |
| `doc-examples/example-python/build.gradle` (`mergePythonSources`) | The documentation classes live in `src/main/python` and the tests in `src/test/python`; compiling them separately yields two GraalPy VFS roots whose generated shim modules shadow each other at test time, and the Python compiler resolves the imports of a source file only within its own source root, so both roots are merged into one directory compiled by `compileTestPython`. |
| `example.PythonRuntimeInitializer` (Java, `src/test/java`) | `@Executable(processOnStartup = true)` processors such as the Pulsar consumer processor are created before the `@Context` beans, so the Python `@PulsarSubscription` beans they instantiate would be created before the GraalPy runtime exists (`GraalPy context has not been initialized`); the initializer is a `TypeConverterRegistrar` injecting the GraalPy context, which is created before those processors. |
| `io.micronaut.pulsar.shared.PulsarTestConfigurer` (Java, `test-suite:test-pulsar-shared-module`) | `TestPropertyProvider.getProperties()` is called by Micronaut Test before the application context, and with it the GraalPy runtime, exists, so a Python test class cannot provide the container's `pulsar.service-url`; the `@ContextConfigurer` adding the property source in `configure(ApplicationContext)` is written in Java. |

## Intentionally Unsupported Snippet Targets

None.

## java.type usages

None.
