package example.listeners;

import io.micronaut.pulsar.annotation.PulsarConsumer;
import io.micronaut.pulsar.annotation.PulsarSubscription;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.subjects.AsyncSubject;
import io.reactivex.subjects.PublishSubject;
import io.reactivex.subjects.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;

@PulsarSubscription(subscriptionName = "reports")
public class ReportsTracker {

    private static final Logger LOG = LoggerFactory.getLogger(ReportsTracker.class);
    private final Subject<String> messageTracker = PublishSubject.create();

    public ReportsTracker() {
        messageTracker.subscribe();
    }

    @PulsarConsumer(consumerName = "report-listener", topic = "persistent://private/reports/messages")
    public void report(String message) {
        messageTracker.onNext(message);
    }

    public Flowable<String> subscribe() {
        return messageTracker.toFlowable(BackpressureStrategy.LATEST);
    }
}
