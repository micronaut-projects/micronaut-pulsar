/*
 * Copyright 2017-2021 original authors
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
package io.micronaut.pulsar.processor;

import io.micronaut.inject.ExecutableMethod;
import kotlin.coroutines.EmptyCoroutineContext;
import kotlinx.coroutines.BuildersKt;
import kotlinx.coroutines.CoroutineScope;
import kotlinx.coroutines.CoroutineScopeKt;
import kotlinx.coroutines.CoroutineStart;

import java.util.Arrays;

/**
 * Kotlin helper class that bridges Java calls from Pulsar Java library into suspend calls to Kotlin.
 *
 * @author Haris Secic
 * @since 1.0
 */
public final class ListenerKotlinHelper {
    public static <T, R> Object run(final ExecutableMethod<T, R> method, final T invoker, final Object... args) {
        final Object[] allArgs = Arrays.copyOf(args, args.length + 1);
        final CoroutineScope scope = CoroutineScopeKt.CoroutineScope(EmptyCoroutineContext.INSTANCE);
        return BuildersKt.launch(scope, EmptyCoroutineContext.INSTANCE, CoroutineStart.DEFAULT, (s, continuation) -> {
            allArgs[args.length] = continuation;
            return method.invoke(invoker, allArgs);
        });
    }
}
