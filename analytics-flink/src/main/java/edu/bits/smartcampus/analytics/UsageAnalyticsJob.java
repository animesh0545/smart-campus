package edu.bits.smartcampus.analytics;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/**
 * Smart Campus Usage Analytics: requirement (e).
 *
 * <pre>
 *  Kafka booking-events ──► parse ──► keyBy(resourceId)
 *        ──► 1-minute tumbling window (stateful AggregateFunction per resource)
 *        ──► JSON result ──► Kafka analytics-results
 * </pre>
 *
 * Per resource and window: bookings, modifications, cancellations, conflicts,
 * occupancy minutes, distinct users and utilisation %. Those are the room
 * occupancy, equipment utilisation and booking-conflict metrics of the
 * Usage Analytics service.
 *
 * Processing-time windows fire every minute even when traffic is sparse (an
 * event-time window waits for watermarks, which stall on idle partitions).
 * Window state is checkpointed every 30s (exactly-once state, at-least-once sink).
 *
 * Arguments:
 *   --bootstrap       MSK bootstrap brokers (required)
 *   --input-topic     default booking-events
 *   --output-topic    default analytics-results
 *   --window-seconds  default 60
 *   --checkpoint-dir  e.g. s3://bucket/flink-checkpoints (optional)
 *   --group-id        default usage-analytics-flink
 */
public class UsageAnalyticsJob {

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        String bootstrap = params.getRequired("bootstrap");
        String inputTopic = params.get("input-topic", "booking-events");
        String outputTopic = params.get("output-topic", "analytics-results");
        long windowSeconds = params.getLong("window-seconds", 60L);
        String groupId = params.get("group-id", "usage-analytics-flink");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        env.enableCheckpointing(30_000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10_000L);
        if (params.has("checkpoint-dir")) {
            env.getCheckpointConfig().setCheckpointStorage(params.get("checkpoint-dir"));
        }

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(inputTopic)
                .setGroupId(groupId)
                // resume from committed offsets; first start reads only new events
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("partition.discovery.interval.ms", "60000")
                .build();

        DataStream<BookingEvent> events = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "kafka:" + inputTopic)
                .uid("kafka-source")
                .flatMap(new ParseEvent())
                .name("parse-booking-event")
                .uid("parse");

        DataStream<String> results = events
                .keyBy(e -> e.resourceId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(windowSeconds)))
                .aggregate(new UsageAggregator(), new WindowResultFunction())
                .name(windowSeconds + "s-usage-window-per-resource")
                .uid("usage-window");

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrap)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(outputTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        results.sinkTo(sink).name("kafka:" + outputTopic).uid("kafka-sink");

        env.execute("smart-campus-usage-analytics");
    }
}
