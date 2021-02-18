package longridealerts;

import common.datatypes.TaxiRide;
import common.sources.TaxiRideGenerator;
import common.utils.ExerciseBase;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Solution to the "Long Ride Alerts" exercise of the Flink training in the docs.
 *
 * The goal for this exercise is to emit START events for taxi rides that have not been matched
 * by an END event during the first 2 hour of the ride
 *
 * @author hzh
 * @version 1.0
 * @date 2021/2/4 上午11:05
 */
public class LongRideAlerts extends ExerciseBase {

    public static void main(String[] args) throws Exception {
        // Set up streaming execution environment, including Web UI and REST endpoint.
        Configuration conf = new Configuration();
        conf.setInteger(RestOptions.PORT, 8050);
        conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(ExerciseBase.parallelism);

        // start the data generator
        DataStream<TaxiRide> rides = env.addSource(rideSourceOrTest(new TaxiRideGenerator()));

        DataStream<TaxiRide> longRides = rides
                .keyBy((TaxiRide ride) -> ride.rideId)
                .process(new MatchFunction());

        printOrTest(longRides);

        env.execute("Long Taxi Rides");
    }

    private static class MatchFunction extends KeyedProcessFunction<Long, TaxiRide, TaxiRide> {

        private ValueState<TaxiRide> rideState;

        @Override
        public void open(Configuration config) throws Exception {
            ValueStateDescriptor<TaxiRide> stateDescriptor = new ValueStateDescriptor<>("rode event", TaxiRide.class);

            rideState = getRuntimeContext().getState(stateDescriptor);
        }

        @Override
        public void processElement(TaxiRide ride, Context context, Collector<TaxiRide> collector) throws Exception {
            TaxiRide previousRideEvent = rideState.value();

            if (previousRideEvent == null) {
                rideState.update(ride);
            } else {
                if (!ride.isStart) {
                    // it's an END event, so event saved was the START event and has a timer
                    // the timer hasn't fired yet, and we can safely kill the timer
                    context.timerService().deleteEventTimeTimer(getTimerTime(previousRideEvent));
                }
                // both events have now been seen, we can clear the state
                rideState.clear();
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<TaxiRide> out) throws Exception {

            // if we get here, we know that the ride started two hours ago, and the END hasn't been processed
            out.collect(rideState.value());
            rideState.clear();
        }

        private long getTimerTime(TaxiRide ride) {
            return ride.startTime.plusSeconds(12 * 60).toEpochMilli();
        }

    }
}
