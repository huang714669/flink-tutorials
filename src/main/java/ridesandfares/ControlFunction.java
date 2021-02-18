package ridesandfares;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @author hzh
 * @version 1.0
 * @date 2021/2/3 下午1:54
 */
public class ControlFunction extends RichCoFlatMapFunction<String, String, String> {

    private ValueState<Boolean> blocked;

    @Override
    public void open(Configuration parameters) throws Exception {
        blocked = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("blocked", Boolean.class));
    }

    @Override
    public void flatMap1(String s, Collector<String> collector) throws Exception {
        blocked.update(Boolean.TRUE);
    }

    @Override
    public void flatMap2(String data_value, Collector<String> collector) throws Exception {
        if (blocked.value() == null) {
            collector.collect(data_value);
        }
    }
}
