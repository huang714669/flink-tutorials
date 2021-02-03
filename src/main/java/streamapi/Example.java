package streamapi;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author hzh
 * @version 1.0
 * @date 2021/2/1 下午2:46
 */
public class Example {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Person> flintstones = env.fromElements(
                new Person("Fred", 35),
                new Person("Wilma", 35),
                new Person("Pebbles", 2)
        );

        SingleOutputStreamOperator<Person> adults = flintstones.filter((FilterFunction<Person>) person -> person.age >= 18);

        adults.print();

        env.execute();
    }
    public static class Person {
        public String name;
        public Integer age;
        public Person() {};

        public Person(String name, Integer age) {
            this.name = name;
            this.age = age;
        };

        @Override
        public String toString() {
            return this.name.toString() + ": age " + this.age.toString();
        };
    }
}
