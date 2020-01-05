package master2019.flink.YellowTaxiTrip;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import org.apache.*;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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


        //VendorID, day, numberOfTrips, tpep_pickup_datetime, tpep_dropoff_datetime
        //Integer, String, Integer, String, String
        SingleOutputStreamOperator<Tuple5<Integer, String, Integer, String, String>> mapStream = text
                .map( new MapFunction<String, Tuple5<Integer, String, Integer, String, String>>() {
                    public Tuple5<Integer, String, Integer, String, String>map(String in) throws Exception{
                    String[] fieldArray = in.split(",");
                    int numberOfTrips=0;
                    Tuple5<Integer, String, Integer, String, String> out= new Tuple5(Integer.parseInt(fieldArray[0]), fieldArray[1], numberOfTrips, fieldArray[1], fieldArray[2]);
                    return out;
                    }
                })
                .filter(new FilterFunction<Tuple5<Integer, String, Integer, String, String>>() {
                    public boolean filter(Tuple5<Integer, String, Integer, String, String> in) throws Exception {
                        SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy", Locale.ENGLISH);
                        Date firstDate = sdf.parse(in.f3);
                        Date secondDate = sdf.parse(in.f4);
                        long diffInMillies = Math.abs(secondDate.getTime() - firstDate.getTime());
                        long min = diffInMillies/60000;
                        return (in.f2>=5 && min>=20);
                    }
                });
        // emit result
        if (params.has("output")) {
            mapStream.writeAsCsv(params.get("output"),FileSystem.WriteMode.OVERWRITE);
        }
        env.execute("Map");

    }
}
