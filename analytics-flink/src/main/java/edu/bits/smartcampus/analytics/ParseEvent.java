package edu.bits.smartcampus.analytics;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** JSON string -> BookingEvent. Malformed records are logged and dropped (never fail the job). */
public class ParseEvent extends RichFlatMapFunction<String, BookingEvent> {
    private static final Logger LOG = LoggerFactory.getLogger(ParseEvent.class);
    private transient ObjectMapper mapper;

    @Override
    public void open(Configuration parameters) {
        mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public void flatMap(String value, Collector<BookingEvent> out) {
        try {
            BookingEvent e = mapper.readValue(value, BookingEvent.class);
            if (e.resourceId != null && e.type != null) {
                out.collect(e);
            }
        } catch (Exception ex) {
            LOG.warn("Dropping malformed booking event: {}", value);
        }
    }
}
