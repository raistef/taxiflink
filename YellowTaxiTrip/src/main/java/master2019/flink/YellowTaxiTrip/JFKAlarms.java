package master2019.flink.YellowTaxiTrip;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.Locale;

/**
 * In this class the JFK airport trips program has to be implemented.
 */
public class JFKAlarms {
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // get input data
        DataStream<String> text;
        // read the text file from given input path
        text = env.readTextFile(params.get("input"));
        env.getConfig().setGlobalJobParameters(params);

        SingleOutputStreamOperator<Tuple5<Integer, String, String, Integer, Integer>> mapStream = text
                .map(new MapFunction<String, Tuple5<Integer, String, String, Integer, Integer>>() {
                    public Tuple5<Integer, String, String, Integer, Integer> map(String in) throws Exception {
                        String[] fieldArray = in.split(",");
                        Tuple5<Integer, String, String, Integer, Integer> out = new Tuple5(Integer.parseInt(fieldArray[0]), fieldArray[1], fieldArray[2], Integer.parseInt(fieldArray[3]), Integer.parseInt(fieldArray[5]));
                        return out;
                    }
                })
                .filter(new FilterFunction<Tuple5<Integer, String, String, Integer, Integer>>() {
                    public boolean filter(Tuple5<Integer, String, String, Integer, Integer> in) throws Exception {
                        return (in.f4 == 2 && in.f3 >= 2);
                    }
                });
        KeyedStream<Tuple5<Integer, String, String, Integer, Integer>,Tuple> keyedStream= mapStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple5<Integer, String, String, Integer, Integer>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple5<Integer, String, String, Integer, Integer> element) {
                        DateTimeFormatter sdf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);
                        LocalDateTime time = LocalDateTime.parse(element.f1, sdf);
                        return time.toEpochSecond(ZoneOffset.UTC)*1000;
                        }
                    }).keyBy(0);
        SingleOutputStreamOperator<Tuple4<Integer, String, String,Integer>> finalStream =  keyedStream.window(TumblingEventTimeWindows.of(Time.hours(1)))
                    .reduce(new ReduceFunction<Tuple5<Integer, String, String, Integer, Integer>>() {
                     @Override
                        public Tuple5<Integer, String, String, Integer, Integer> reduce(Tuple5<Integer, String, String, Integer, Integer> v1, Tuple5<Integer, String, String, Integer, Integer> v2) throws Exception {
                         DateTimeFormatter sdf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);
                         LocalDateTime time1 = LocalDateTime.parse(v1.f2, sdf);
                         LocalDateTime time2 = LocalDateTime.parse(v2.f2, sdf);
                        String endTime= time1.isBefore(time2) ? v2.f2 : v1.f2;

                         if(v2!=null)
                            return new Tuple5<Integer, String, String, Integer, Integer>(v1.f0,v1.f1,endTime,v1.f3+v2.f3,v1.f4);
                        else
                            return new Tuple5<Integer, String, String, Integer, Integer>(v1.f0,v1.f1,endTime,v1.f3,v1.f4);
                    }
                })
                .map(new MapFunction<Tuple5<Integer, String, String, Integer, Integer>, Tuple4<Integer, String, String, Integer>>() {
                    public Tuple4<Integer, String, String, Integer> map(Tuple5<Integer, String, String, Integer, Integer> in) throws Exception {
                    return new Tuple4<Integer, String, String, Integer>(in.f0,in.f1,in.f2,in.f3) ;
                }
            });
        // emit result
        if (params.has("output")) {
            finalStream.writeAsCsv(params.get("output"),FileSystem.WriteMode.OVERWRITE);
        }
        env.execute();

    }

}
