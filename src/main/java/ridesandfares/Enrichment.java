package ridesandfares;

import common.datatypes.TaxiRide;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author hzh
 * @version 1.0
 * @date 2021/2/2 下午4:28
 */
public class Enrichment  implements MapFunction<TaxiRide, EnrichedRide> {
    @Override
    public EnrichedRide map(TaxiRide taxiRide) throws Exception {
        return new EnrichedRide(taxiRide);
    }
}
