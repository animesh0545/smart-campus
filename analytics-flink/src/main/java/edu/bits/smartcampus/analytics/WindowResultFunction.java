package edu.bits.smartcampus.analytics;

import java.time.Instant;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Adds the window bounds to the pre-aggregated accumulator and renders the
 * JSON record published to the {@code analytics-results} topic.
 * Also logs one WINDOW_RESULT line per window, which the EMR CloudWatch agent
 * ships to CloudWatch Logs (and Grafana).
 */
public class WindowResultFunction
        extends ProcessWindowFunction<UsageAccumulator, String, String, TimeWindow> {

    private static final Logger LOG = LoggerFactory.getLogger(WindowResultFunction.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    /** Bookable day used for utilisation: 08:00-22:00 = 840 minutes. */
    static final double BOOKABLE_MINUTES_PER_DAY = 840.0;

    @Override
    public void process(String resourceId, Context ctx, Iterable<UsageAccumulator> results,
                        Collector<String> out) {
        UsageAccumulator acc = results.iterator().next();
        String json = render(resourceId, ctx.window().getStart(), ctx.window().getEnd(), acc);
        LOG.info("WINDOW_RESULT {}", json);
        out.collect(json);
    }

    static String render(String resourceId, long windowStartMs, long windowEndMs, UsageAccumulator acc) {
        ObjectNode n = MAPPER.createObjectNode();
        n.put("resourceId", resourceId);
        n.put("resourceType", acc.resourceType);
        n.put("windowStart", Instant.ofEpochMilli(windowStartMs).toString());
        n.put("windowEnd", Instant.ofEpochMilli(windowEndMs).toString());
        n.put("bookings", acc.bookings);
        n.put("modifications", acc.modifications);
        n.put("cancellations", acc.cancellations);
        n.put("conflicts", acc.conflicts);
        n.put("occupancyMinutes", acc.occupancyMinutes);
        n.put("uniqueUsers", acc.users.size());
        // share of one bookable day that was booked during this window
        double util = 100.0 * Math.max(0, acc.occupancyMinutes) / BOOKABLE_MINUTES_PER_DAY;
        n.put("utilisationPct", Math.round(util * 100.0) / 100.0);
        n.put("producedBy", "flink-usage-analytics");
        return n.toString();
    }
}
