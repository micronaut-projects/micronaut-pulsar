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
package example.listeners;

import io.micronaut.pulsar.annotation.PulsarConsumer;
import io.micronaut.pulsar.annotation.PulsarSubscription;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.subjects.PublishSubject;
import io.reactivex.subjects.Subject;
import org.apache.pulsar.client.api.SubscriptionType;

/**
 * Reports topic consumer that outputs data as async flow on request.
 */
@PulsarSubscription(subscriptionName = "reports", subscriptionType = SubscriptionType.Shared)
public class ReportsTracker {

    private final Subject<String> messageTracker = PublishSubject.create();

    public ReportsTracker() {
        messageTracker.subscribe();
    }

    /**
     * @param message string to store async
     */
    @PulsarConsumer(consumerName = "report-listener", topic = "persistent://private/reports/messages")
    public void report(String message) {
        messageTracker.onNext(message);
    }

    /**
     * Consume messages as they come in from pulsar. Creates a "reactive client" that takes in messages as they come in.TO
     * @return flow of message strings
     */
    public Flowable<String> subscribe() {
        return messageTracker.toFlowable(BackpressureStrategy.LATEST);
    }
}
