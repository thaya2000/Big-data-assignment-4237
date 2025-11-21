package com.example.kafka.aggregation;

import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;

/**
 * Maintains a running average of order prices.
 */
@Component
public class RunningAverageCalculator {

    private final DoubleAdder totalAmount = new DoubleAdder();
    private final LongAdder orderCount = new LongAdder();

    public double addAmount(double amount) {
        totalAmount.add(amount);
        orderCount.increment();
        return getCurrentAverage();
    }

    public double getCurrentAverage() {
        long count = orderCount.sum();
        if (count == 0) {
            return 0.0;
        }
        return totalAmount.sum() / count;
    }

    public long getOrderCount() {
        return orderCount.sum();
    }

    public double getTotalAmount() {
        return totalAmount.sum();
    }

    public String getStatistics() {
        return String.format("orders=%d, total=%.2f, runningAverage=%.2f",
                getOrderCount(), getTotalAmount(), getCurrentAverage());
    }

    public void reset() {
        totalAmount.reset();
        orderCount.reset();
    }
}
