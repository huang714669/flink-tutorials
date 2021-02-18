package ridecleansing;

import common.datatypes.TaxiRide;
import common.sources.TaxiRideGenerator;
import common.utils.ExerciseBase;
import common.utils.GeoUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author hzh
 * @version 1.0
 * @date 2021/2/2 下午2:31
 * Solution to the "Ride Cleansing" exercise of the Flink training in the docs.
 *
 * <p>The task of the exercise is to filter a data stream of taxi ride records to keep only rides that
 * start and end within New York City. The resulting stream should be printed.
 */

public class RideCleansing extends ExerciseBase {

    /**
     * Main Method
     *
     * @param args
     * @throws Exception occurs during job execution
     */
    public static void main(String[] args) throws Exception {
        // web ui config
        Configuration conf = new Configuration();
        conf.setInteger(RestOptions.PORT, 8050);

        // set up stream execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(ExerciseBase.parallelism);

        // start data generator
        DataStreamSource<TaxiRide> rides = env.addSource(rideSourceOrTest(new TaxiRideGenerator()));

        SingleOutputStreamOperator<TaxiRide> filterdRides = rides
                // keep only those rides and both start and end in NYk
                .filter(new NYKFilter());

        // print the filtered stream
        printOrTest(filterdRides);

        // run the cleasing pipeline
        env.execute("Taxi Ride Cleasing");

    }

    public static class NYKFilter implements org.apache.flink.api.common.functions.FilterFunction<TaxiRide> {
        @Override
        public boolean filter(TaxiRide taxiRide) throws Exception {
            return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat) && GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
        }
    }
}
