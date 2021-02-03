package ridesandfares;

import common.datatypes.TaxiRide;
import common.sources.TaxiRideGenerator;
import common.utils.ExerciseBase;
import common.utils.GeoUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import ridecleansing.RideCleansing;

import java.time.temporal.ChronoUnit;

/**
 * @author hzh
 * @version 1.0
 * @date 2021/2/2 下午3:59
 */
public class RidesAndFaresExercise {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger(RestOptions.PORT, 8050);
        conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(ExerciseBase.parallelism);

        // start the data generator
        DataStream<TaxiRide> rides = env.addSource(new TaxiRideGenerator());

//        mapExercise(env, rides);
//        flatMapExercise(env, rides);
//        keyByExercise(env, rides);
//        keyedAggExercise(env, rides);
        connectedStreamExercise(env, rides);
    }

    /**
     * 使用map方法为每个出租车行程对象添加startCell和endCell字段
     *
     * @param env
     * @param rides
     * @throws Exception
     */
    static void mapExercise(StreamExecutionEnvironment env, DataStream<TaxiRide> rides) throws Exception {
        DataStream<EnrichedRide> enrichedNYCRides = rides
                .filter(new RideCleansing.NYKFilter())
                .map(new Enrichment());

        enrichedNYCRides.print();

        env.execute("map exercise");
    }

    static void flatMapExercise(StreamExecutionEnvironment env, DataStream<TaxiRide> rides) throws Exception {
        DataStream<EnrichedRide> enrichedNYCRides = rides.flatMap(new NYCEnrichment());
        enrichedNYCRides.print();

        env.execute("flat map exercise");
    }

    static void keyByExercise(StreamExecutionEnvironment env, DataStream<TaxiRide> rides) throws Exception {
        KeyedStream<EnrichedRide, Integer> keyedNYCRides = rides
                .flatMap(new NYCEnrichment())
                .keyBy(ride -> GeoUtils.mapToGridCell(ride.startLon, ride.startLat));
//                .keyBy(enrichedRide -> enrichedRide.startCell);

        keyedNYCRides.print();

        env.execute("key by exercise");
    }

    static void keyedAggExercise(StreamExecutionEnvironment env, DataStream<TaxiRide> rides) throws Exception {
        DataStream<EnrichedRide> enrichedNYCRides = rides
                .filter(new RideCleansing.NYKFilter())
                .map(new Enrichment());
        DataStream<Tuple2<Integer, Long>> minutesByStartCell = enrichedNYCRides
                .flatMap(new FlatMapFunction<EnrichedRide, Tuple2<Integer, Long>>() {
                    @Override
                    public void flatMap(EnrichedRide ride, Collector<Tuple2<Integer, Long>> collector) throws Exception {
                        if (!ride.isStart) {
                            Long duration = ChronoUnit.MINUTES.between(ride.startTime, ride.endTime);
                            collector.collect(new Tuple2<>(ride.startCell, duration));
                        }
                    }
                });
        minutesByStartCell
                .keyBy(value -> value.f0) // startCell
                .maxBy(1) // duration
                .print();

        env.execute("keyed agg exercise");
    }

    static void connectedStreamExercise(StreamExecutionEnvironment env, DataStream<TaxiRide> rides) throws Exception {
        KeyedStream<String, String> control = env.fromElements("DROP", "IGNORE").keyBy(x -> x);
        KeyedStream<String, String> streamOfWords = env.fromElements("Apache", "DROP", "Flink", "IGNORE").keyBy(x -> x);

        control
                .connect(streamOfWords)
                .flatMap(new ControlFunction())
                .print();
        env.execute("connected stream exercise");
    }

}
