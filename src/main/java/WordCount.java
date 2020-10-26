import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Function2;
import scala.Tuple2;

import java.io.File;
import java.util.*;

public class WordCount {

  public static void studyRDD(JavaSparkContext sc){
    // parallelize
    JavaRDD<String> javaStringRDD = sc.parallelize(Arrays.asList("shenzhen", "is a beautiful city"),2);
    // TextFile
    JavaRDD<String> textFile = sc.textFile("testdata/shakespeare.txt",3);
    System.out.println("TextFile:"+   textFile.collect().toString());
    // Filter
    JavaRDD<String> lines = sc.textFile("testdata/sampe.txt");
    JavaRDD<String> zksRDD = lines.filter(new Function<String, Boolean>() {
      @Override
      public Boolean call(String s) throws Exception {
        return s.contains("zks");
      }
    });

    List<String> zksCollect = zksRDD.collect();
    for (String str:zksCollect) {
      System.out.println(">>>>>>>>>> filter >>>>>>>>>>"+str);
    }
    // map
    JavaRDD<Iterable<String>> mapRDD = lines.map(new Function<String, Iterable<String>>() {
      @Override
      public Iterable<String> call(String s) throws Exception {
        String[] split = s.split("\\s+");
        return Arrays.asList(split);
      }
    });

    //读取第一个元素
    System.out.println(">>>>>>>>>> map first >>>>>>>>>>"+mapRDD.first() +" map Collect :"+mapRDD.collect() );

      JavaRDD<String> flatMapRDD = lines.flatMap(new FlatMapFunction<String, String>() {
          @Override
          public Iterator<String> call(String s) throws Exception {
              String[] split = s.split("\\s+");
              return Arrays.asList(split).iterator();
          }
      });
      //输出第一个
      System.out.println(">>>>>>>>>> flatMap first >>>>>>>>>>"+flatMapRDD.first() +" flatMap Collect: "+flatMapRDD.collect());

    // distinct
    JavaRDD<String> RDD1 = sc.parallelize(Arrays.asList("aa", "aa", "bb", "cc", "dd"));
    JavaRDD<String> distinctRDD = RDD1.distinct();
    List<String> collect = distinctRDD.collect();
    System.out.println(">>>>>>>>>> distinct collect >>>>>>>>>>"+collect);

    // union
    JavaRDD<String> RDD11 = sc.parallelize(Arrays.asList("aa", "aa", "bb", "cc", "dd"));
    JavaRDD<String> RDD22 = sc.parallelize(Arrays.asList("aa","dd","ff"));
    JavaRDD<String> unionRDD = RDD11.union(RDD22);
    List<String> collect33 = unionRDD.collect();
    System.out.println(">>>>>>>>>> distinct collect >>>>>>>>>>"+collect33);

    // intersection
    JavaRDD<String> RDD111 = sc.parallelize(Arrays.asList("aa", "aa", "bb", "cc", "dd"));
    JavaRDD<String> RDD222 = sc.parallelize(Arrays.asList("aa","dd","ff"));
    JavaRDD<String> intersectionRDD = RDD1.intersection(RDD222);
    List<String> collect333 = intersectionRDD.collect();
    System.out.println(">>>>>>>>>> intersection collect >>>>>>>>>>"+collect333);

    // substract
    JavaRDD<String> RDD1111 = sc.parallelize(Arrays.asList("aa", "aa", "bb","cc", "dd"));
    JavaRDD<String> RDD2222 = sc.parallelize(Arrays.asList("aa","dd","ff"));
    JavaRDD<String> subtractRDD = RDD1.subtract(RDD2222);
    List<String> collect3333 = subtractRDD.collect();
    System.out.println(">>>>>>>>>> substract collect >>>>>>>>>>"+collect3333);

    // cartesian
    JavaRDD<String> RDD11111 = sc.parallelize(Arrays.asList("1", "2", "3"));
    JavaRDD<String> RDD22222 = sc.parallelize(Arrays.asList("a","b","c"));
    JavaPairRDD<String, String> cartesian = RDD11111.cartesian(RDD22222);

    List<Tuple2<String, String>> collecta = cartesian.collect();
    System.out.println(">>>>>>>>>> cartesian collect >>>>>>>>>>"+collecta);

    // mapToPair
    // 输入的是一个string的字符串，输出的是一个(String, Integer) 的map
   lines = sc.textFile("testdata/sampe2.txt");
    JavaPairRDD<String, Integer> pairRDD = lines.mapToPair(new PairFunction<String, String, Integer>() {
      @Override
      public Tuple2<String, Integer> call(String s) throws Exception {
        return new Tuple2<String, Integer>(

                s.split("\\s+")[0], 1);
      }
    });
    System.out.println(">>>>>>>>>> mapToPair collect >>>>>>>>>>"+pairRDD.collect());

    // flatMapToPair
    JavaPairRDD<String, Integer> wordPairRDD = lines.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
      @Override
      public Iterator<Tuple2<String, Integer>> call(String s) throws Exception {
        ArrayList<Tuple2<String, Integer>> tpLists = new ArrayList<Tuple2<String, Integer>>();
        String[] split = s.split("\\s+");
        for (int i = 0; i <split.length ; i++) {
          Tuple2 tp = new Tuple2<String,Integer>(split[i], 1);
          tpLists.add(tp);
        }
        return tpLists.iterator();
      }
    });

    System.out.println(">>>>>>>>>> flatMapToPair collect >>>>>>>>>>"+wordPairRDD.collect());


   wordPairRDD = lines.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
      @Override
      public Iterator<Tuple2<String, Integer>> call(String s) throws Exception {
        ArrayList<Tuple2<String, Integer>> tpLists = new ArrayList<Tuple2<String, Integer>>();
        String[] split = s.split("\\s+");
        for (int i = 0; i <split.length ; i++) {
          Tuple2 tp = new Tuple2<String,Integer>(split[i], 1);
          tpLists.add(tp);
        }
        return tpLists.iterator();
      }
    });


//    JavaPairRDD<String, Integer> wordCountRDD = wordPairRDD.reduceByKey(
//            new Function2<Integer, Integer, Integer>() {
//              @Override
//              public Integer call(Integer i1, Integer i2) throws Exception {
//                return i1 + i2;
//              }}
//              );
//
//    Map<String, Integer> collectAsMap = wordCountRDD.collectAsMap();
//    for (String key:collectAsMap.keySet()) {
//      System.out.println("("+key+","+collectAsMap.get(key)+")");
//    }


  }


  public static void main(String[] args) throws Exception {

    FileUtils.deleteDirectory(new File("testdata/words_java.txt"));

    SparkConf sparkConf = new SparkConf()
        .setAppName("Example")
        .setMaster("local[*]");

    JavaSparkContext sc = new JavaSparkContext(sparkConf);

    studyRDD(sc);

    // open the text file as an RDD of String
    JavaRDD<String> textFile = sc.textFile("testdata/shakespeare.txt");

    // convert each line into a collection of words
    JavaRDD<String> words = textFile.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterator<String> call(String s) throws Exception {
        return Arrays.asList(WordHelper.split(s)).iterator();
      }
    });

    // map each word to a tuple containing the word and the value 1
    JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
      @Override
      public Tuple2<String, Integer> call(String word) { return new Tuple2<>(word, 1); }
    });

    System.out.println(">>>>>>>>>>> first "+pairs.first().toString() );

    // for all tuples that have the same key (word), perform an aggregation to add the counts
    JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new org.apache.spark.api.java.function.Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer a, Integer b) throws Exception {
        return a + b;
      }
    });
    System.out.println(">>>>>>>>>>> counts "+counts.first().toString() );


    // perform some final transformations, and then save the output to a file

    counts.filter(tuple -> tuple._2() > 0 )
            .saveAsTextFile("testdata/words_java.txt");



  }

}
