package master2019.flink.YellowTaxiTrip;

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

        SingleOutputStreamOperator<Tuple5<Integer, String, String,Integer,Integer>> mapStream = text
                .map( new MapFunction<String, Tuple5<Integer, String, String,Integer,Integer>>() {
                    public Tuple5<Integer, String, String,Integer,Integer>map(String in) throws Exception{
                    String[] fieldArray = in.split(",");
                    Tuple5<Integer, String, String,Integer,Integer> out= new Tuple5(Integer.parseInt(fieldArray[0]), fieldArray[1],fieldArray[2], Integer.parseInt(fieldArray[3]),Integer.parseInt(fieldArray[5]));
                    return out;
                    }
                })
                .filter(new FilterFunction<Tuple5<Integer, String, String,Integer,Integer>>() {
                    public boolean filter(Tuple5<Integer, String, String,Integer,Integer> in) throws Exception {
                        return (in.f4==1 && in.f3>=2);
                    }
                });
        SingleOutputStreamOperator<Tuple4<Integer, String, String,Integer>> finalStream = mapStream
                .map(new MapFunction<Tuple5<Integer, String, String, Integer, Integer>, Tuple4<Integer, String, String, Integer>>() {
                    public Tuple4<Integer, String, String, Integer> map(Tuple5<Integer, String, String, Integer, Integer> in) throws Exception {
                    return new Tuple4<Integer, String, String, Integer>(in.f0,in.f1,in.f2,in.f3) ;
                }
            });
        // emit result
        if (params.has("output")) {
            finalStream.writeAsCsv(params.get("output"),FileSystem.WriteMode.OVERWRITE);
        }
        env.execute("Map");

    }
}
