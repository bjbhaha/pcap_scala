import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.file.tfile.ByteArray;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


public class pcaptest{
    public static void toIp(byte[] ip){

    }
//    public static void printByte(ByteArray ba){
//        for(int i=0;i<ba.size();i++){
//            System.out.print(ba.)
//        }
//    }
class MaxLength implements Function2<byte[], byte[],byte[]> {
    @Override
    public byte[] call(byte[] bytes, byte[] bytes2) throws Exception {
        if(bytes.length>bytes2.length)  return bytes;
        else return bytes2;
    }
}

    public static String binary(byte[] bytes, int radix){
        return new BigInteger(1, bytes).toString(radix);// 这里的1代表正数
    }
    public static void main(String args[]){
        SparkConf conf = new SparkConf().setAppName("pcaptest").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaPairRDD<IntWritable, BytesWritable> javaRDD =sc.sequenceFile("/home/bjbhaha/Desktop/music3.seq", IntWritable.class, BytesWritable.class);
        //JavaPairRDD<IntWritable,Text> javaRDD2=javaRDD.ToPair(v2->v2);
        //javaRDD.foreach(x -> x._2.get());
        //javaRDD.cache();
       // JavaPairRDD<IntWritable, ByteArray> javaRDD2=

//        byte[] a={new byte("cf"),c,d};
//        int b=a.length();
        //List<Byte> lista= new ArrayList<Byte>() {};
        //ByteArray ba=
        //List<BytesWritable> list4=new ArrayList<BytesWritable>();


        List<String> aa=new ArrayList<>();

        //int []aaa=new int[1];
        //System.out.println("aa:");javaRDD.foreach(x-> aaa[0]=x._1().get());
        int i=0;
        System.out.println("a:");javaRDD.foreach(x-> System.out.println(i+" "+x._1()+" "+x._2()));

        //javaRDD.foreach(x-> aa.add(binary(x._2().getBytes(),16)));

       // List<Tuple2<IntWritable, BytesWritable>> aa1=javaRDD.collect();
        //System.out.println("a:");javaRDD.foreach(x-> aa.add(new ByteArray(x._2())));
        //List<byte[]> list4=new ArrayList<>();
        //System.out.println("a:");javaRDD.foreach(x->System.out.println(x._2.getBytes()));

        /*JavaRDD<String> rdd3=javaRDD.map(x ->new String(x._2.getBytes(),0,x._2.getLength(),"ISO8859-1"));
        List<String> aa2=rdd3.collect();
        aa2.forEach(x-> {
            try {
                System.out.println(binary(x.getBytes("ISO8859-1"),16));
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        });*/
/*
        JavaRDD<String> aa3=javaRDD.map(x ->new String(x._2.getBytes(),0,x._2.getLength(),"ISO8859-1"))
                .map(x-> binary(x.getBytes("ISO8859-1"),16)
        );
        class MaxLength2 implements Function2<String, String,String> {
            @Override
            public String call(String s1, String s2) throws Exception {
                if(s1.length()>s2.length())  return s1;
                else return s2;
            }
        }*/
//        String aa4=javaRDD.map(x ->new String(x._2.getBytes(),0,x._2.getLength(),"ISO8859-1"))
//                .map(x-> binary(x.getBytes("ISO8859-1"),16)
//                ).reduce(new MaxLength2());
//        System.out.println(aa4);
/*
        class setLength implements Function<Tuple2<IntWritable,BytesWritable>,String> {
//            @Override
//            public String call(BytesWritable bytesWritable) throws Exception {
//                bytesWritable.setCapacity(bytesWritable.getSize());
//                return new String(bytesWritable.getBytes(),0,bytesWritable.getLength(),"ISO8859-1");
//            }

            @Override
            public String call(Tuple2<IntWritable, BytesWritable> intWritableBytesWritableTuple2) throws Exception {
                intWritableBytesWritableTuple2._2.setCapacity(intWritableBytesWritableTuple2._2.getLength());
                return new String(intWritableBytesWritableTuple2._2.getBytes(),0,intWritableBytesWritableTuple2._2.getLength(),"ISO8859-1");

            }
        }
        String aa5=javaRDD.map(new setLength()).map(x-> binary(x.getBytes("ISO8859-1"),16)
        ).reduce(new MaxLength2());
        System.out.println(aa5);*/
//        List<byte[]> list4;
//        System.out.println("a:");
//        javaRDD.map(x->(x._2));

        //System.out.println("b:");javaRDD.map(x -> x._2.get()).foreach(x ->System.out.println(x));

        //List<byte[]> list=javaRDD.map(x -> x._2().get()).collect();
        /*javaRDD.map(x -> x._2().get()).foreach(x->System.out.println(x));
        JavaRDD rdd=javaRDD.map(x -> x._1().get());*/

        //System.out.println("c:");rdd.foreach(x ->System.out.println(x));
        System.out.println("d:");
//        for(byte[] x : list){
//            for(byte y: x){
//                System.out.print(y+" ");
//            }
//            System.out.println();
//        }
//        for (Map.Entry<IntWritable, BytesWritable> entry: javaRDD.collectAsMap().entrySet()) {
//            System.out.println("entry.getValue()");
//        }
    }
}
