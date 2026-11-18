package edu.bits.smartcampus.analytics;

import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Incremental aggregation: Flink keeps only one small accumulator per
 * (resource, window) instead of buffering every event.
 */
public class UsageAggregator implements AggregateFunction<BookingEvent, UsageAccumulator, UsageAccumulator> {
    @Override
    public UsageAccumulator createAccumulator() {
        return new UsageAccumulator();
    }

    @Override
    public UsageAccumulator add(BookingEvent event, UsageAccumulator acc) {
        acc.add(event);
        return acc;
    }

    @Override
    public UsageAccumulator getResult(UsageAccumulator acc) {
        return acc;
    }

    @Override
    public UsageAccumulator merge(UsageAccumulator a, UsageAccumulator b) {
        return a.merge(b);
    }
}
