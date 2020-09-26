/*
 * Copyright 2017-2020 original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.micronaut.pulsar.annotation;

import javax.inject.Qualifier;
import java.lang.annotation.Documented;
import java.lang.annotation.Retention;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Marks a class that contains method to resolve pulsar service url. Class annotated must implement ServiceUrlProvider
 * interface from official Apache Pulsar Java library.
 *
 * @author Haris Secic
 * @since 1.0
 */
@Documented
@Retention(RUNTIME)
@Qualifier
public @interface PulsarServiceUrlProvider {
    //Just a distinguished annotation for PulsarServiceProvider
}
