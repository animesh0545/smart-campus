package edu.bits.smartcampus.analytics;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * Per-resource window state kept by Flink between events (the "stateful" part
 * of the stateful time-windowed aggregation). Stored in the configured state
 * backend and included in every checkpoint.
 */
public class UsageAccumulator implements Serializable {
    public String resourceType = "UNKNOWN";
    public long bookings;          // CREATED
    public long modifications;     // MODIFIED
    public long cancellations;     // CANCELLED
    public long conflicts;         // CONFLICT (rejected booking attempts)
    public long occupancyMinutes;  // minutes booked by CREATED events (minus cancelled minutes)
    public Set<String> users = new HashSet<>();  // distinct users active on the resource

    public void add(BookingEvent e) {
        if (e.resourceType != null) {
            resourceType = e.resourceType;
        }
        switch (e.type) {
            case "CREATED":
                bookings++;
                occupancyMinutes += Math.max(0, e.durationMinutes);
                break;
            case "MODIFIED":
                modifications++;
                break;
            case "CANCELLED":
                cancellations++;
                occupancyMinutes -= Math.max(0, e.durationMinutes);
                break;
            case "CONFLICT":
                conflicts++;
                break;
            default:
                return;
        }
        if (e.userId != null) {
            users.add(e.userId);
        }
    }

    public UsageAccumulator merge(UsageAccumulator other) {
        if ("UNKNOWN".equals(resourceType)) {
            resourceType = other.resourceType;
        }
        bookings += other.bookings;
        modifications += other.modifications;
        cancellations += other.cancellations;
        conflicts += other.conflicts;
        occupancyMinutes += other.occupancyMinutes;
        users.addAll(other.users);
        return this;
    }
}
