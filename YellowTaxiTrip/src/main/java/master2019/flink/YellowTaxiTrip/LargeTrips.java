package master2019.flink.YellowTaxiTrip;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Locale;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class LargeTrips {
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // get input data
        DataStream<String> text;
        // read the text file from given input path
        text = env.readTextFile(params.get("input"));
        env.getConfig().setGlobalJobParameters(params);

        //ALL
        //VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID,
        //DOLocationID, payment_type, fare_amount, extra, mta_tax,tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge


        //VendorID f0, day f1, numberOfTrips f2, tpep_pickup_datetime f3, tpep_dropoff_datetime f4
        //Integer, String, Integer, String, String
        SingleOutputStreamOperator<Tuple6<Integer, String, Integer, String, String,Integer>> mapStream6 = text
                .map( new MapFunction<String, Tuple6<Integer, String, Integer, String, String,Integer>>() {
                    public Tuple6<Integer, String, Integer, String, String,Integer>map(String in) throws Exception{
                        String[] fieldArray = in.split(",");
                        int numberOfTrips=1;
                        Tuple6<Integer, String, Integer, String, String,Integer> out= new Tuple6(Integer.parseInt(fieldArray[0]), fieldArray[1], numberOfTrips, fieldArray[1], fieldArray[2],Integer.parseInt(fieldArray[3]));
                        return out;
                    }
                })
                .filter(new FilterFunction<Tuple6<Integer, String, Integer, String, String,Integer>>() {
                    @Override
                    public boolean filter(Tuple6<Integer, String, Integer, String, String, Integer> in) throws Exception {
                        DateTimeFormatter sdf =  DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);
                        LocalDateTime firstDate = LocalDateTime.parse(in.f3,sdf);
                        LocalDateTime secondDate = LocalDateTime.parse(in.f4,sdf);
                        long diffInMinutes = java.time.Duration.between(firstDate, secondDate).toMinutes();

                        return (in.f5>=5 && diffInMinutes>=20);
                    }

                });
        SingleOutputStreamOperator<Tuple5<Integer, String, Integer, String, String>> mapStream = mapStream6.map(new MapFunction<Tuple6<Integer, String, Integer, String, String, Integer>, Tuple5<Integer, String, Integer, String, String>>() {
            @Override
            public Tuple5<Integer, String, Integer, String, String> map(Tuple6<Integer, String, Integer, String, String, Integer> in) throws Exception {
                return new Tuple5<Integer, String, Integer, String, String>(in.f0,in.f1,in.f2,in.f3,in.f4);
            }
        });

        KeyedStream<Tuple5<Integer, String, Integer, String, String>,Tuple> keyedStream= mapStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple5<Integer, String, Integer, String, String>>(Time.minutes(59)) {
            @Override
            public long extractTimestamp(Tuple5<Integer, String, Integer, String, String> element) {
                DateTimeFormatter sdf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);
                LocalDateTime time = LocalDateTime.parse(element.f3, sdf);
                return time.toEpochSecond(ZoneOffset.UTC)*1000;
            }
        }).keyBy(0);
        SingleOutputStreamOperator<Tuple5<Integer, String, Integer, String, String>> aggregatedStream =  keyedStream.window(TumblingEventTimeWindows.of(Time.hours(3)))
                .reduce(new ReduceFunction<Tuple5<Integer, String, Integer, String, String>>() {
                    @Override
                    public Tuple5<Integer, String, Integer, String, String> reduce(Tuple5<Integer, String, Integer, String, String> v1, Tuple5<Integer, String, Integer, String, String> v2) throws Exception {
                        DateTimeFormatter sdf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);
                        LocalDateTime time1 = LocalDateTime.parse(v1.f4, sdf);
                        LocalDate onlyDate = time1.toLocalDate();
                        if (v2 != null) {
                            LocalDateTime time2 = LocalDateTime.parse(v2.f4, sdf);
                            String endTime = time1.isBefore(time2) ? v2.f4 : v1.f4;
                            DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                            return new Tuple5<Integer, String, Integer, String, String>(v1.f0, onlyDate.toString(), v1.f2 + v2.f2, v1.f3, endTime);
                        } else
                            return new Tuple5<Integer, String, Integer, String, String>(v1.f0, onlyDate.toString(), v1.f2, v1.f3, v1.f4);
                    }
                });

        SingleOutputStreamOperator<Tuple5<Integer, String, Integer, String, String>> finalStream = aggregatedStream
                .filter(new FilterFunction<Tuple5<Integer, String, Integer, String, String>>() {
                    public boolean filter(Tuple5<Integer, String, Integer, String, String> in) throws Exception {
                        return (in.f2 >= 5);
                    }
                });
        // emit result
        if (params.has("output")) {
            finalStream.writeAsCsv(params.get("output"),FileSystem.WriteMode.OVERWRITE);
        }
        env.execute();

    }
}