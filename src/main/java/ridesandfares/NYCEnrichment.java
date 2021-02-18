package ridesandfares;

import common.datatypes.TaxiRide;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import ridecleansing.RideCleansing;

/**
 * 使用flatmap，可以进行过滤等操作
 * @author hzh
 * @version 1.0
 * @date 2021/2/2 下午4:44
 */
public class NYCEnrichment implements FlatMapFunction<TaxiRide, EnrichedRide> {
    @Override
    public void flatMap(TaxiRide taxiRide, Collector<EnrichedRide> collector) throws Exception {
        RideCleansing.NYKFilter valid = new RideCleansing.NYKFilter();
        if (valid.filter(taxiRide)) {
            collector.collect(new EnrichedRide(taxiRide));
        }
    }
}
