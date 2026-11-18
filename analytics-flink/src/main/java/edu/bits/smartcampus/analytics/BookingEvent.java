package edu.bits.smartcampus.analytics;

import java.io.Serializable;

/**
 * One message from the Kafka topic {@code booking-events}, as published by the
 * Room / Equipment Booking services (see services/common/events.py).
 * Public fields + no-arg constructor = a Flink POJO (efficient serialization).
 */
public class BookingEvent implements Serializable {
    public String eventId;
    public String type;          // CREATED | MODIFIED | CANCELLED | CONFLICT
    public String bookingId;
    public String resourceId;
    public String resourceType;  // ROOM | EQUIPMENT
    public String userId;
    public String date;
    public String startTime;
    public String endTime;
    public int durationMinutes;
    public String partitionNode;
    public String ts;

    public BookingEvent() {}

    @Override
    public String toString() {
        return type + " " + resourceId + " " + date + " " + startTime + "-" + endTime + " by " + userId;
    }
}
