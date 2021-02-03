package ridesandfares;

import common.datatypes.TaxiRide;
import common.utils.GeoUtils;

/**
 * @author hzh
 * @version 1.0
 * @date 2021/2/2 下午4:26
 */
public class EnrichedRide extends TaxiRide {
    public int startCell;
    public int endCell;

    public EnrichedRide() {
    };

    public EnrichedRide(TaxiRide ride) {
        this.rideId = ride.rideId;
        this.taxiId = ride.taxiId;
        this.driverId = ride.driverId;
        this.isStart = ride.isStart;
        this.startTime = ride.startTime;
        this.endTime = ride.endTime;
        this.startLon = ride.startLon;
        this.startLat = ride.startLat;
        this.endLon = ride.endLon;
        this.endLat = ride.endLat;
        this.passengerCnt = ride.passengerCnt;
        this.startCell = GeoUtils.mapToGridCell(ride.startLon, ride.startLat);
        this.endCell = GeoUtils.mapToGridCell(ride.endLon, ride.endLat);
    }

    @Override
    public String toString() {
        return super.toString() + "," +
                this.startCell + "," +
                this.endCell;
    }
}
