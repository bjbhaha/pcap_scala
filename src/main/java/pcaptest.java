import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.file.tfile.ByteArray;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


public class pcaptest{
    public static void toIp(byte[] ip){

    }
    public static void main(String args[]){
        SparkConf conf = new SparkConf().setAppName("pcaptest").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaPairRDD<IntWritable, BytesWritable> javaRDD =sc.sequenceFile("/home/bjbhaha/Desktop/music.seq", IntWritable.class, BytesWritable.class);
        //JavaPairRDD<IntWritable,Text> javaRDD2=javaRDD.ToPair(v2->v2);
        //javaRDD.foreach(x -> x._2.get());
        //javaRDD.cache();
       // JavaPairRDD<IntWritable, ByteArray> javaRDD2=
        List<byte[]> list=javaRDD.map(x -> x._2.get()).collect();
        JavaRDD rdd=javaRDD.map(x -> x._1().get());
        rdd.foreach(x ->System.out.println(x));
        for(byte[] x : list){
            System.out.println(x);
        }
//        for (Map.Entry<IntWritable, BytesWritable> entry: javaRDD.collectAsMap().entrySet()) {
//            System.out.println("entry.getValue()");
//        }
    }
}
