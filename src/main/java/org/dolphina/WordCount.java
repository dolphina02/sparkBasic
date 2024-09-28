package org.dolphina;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {
    public static void main(String[] args) {
        // Spark Configuration 및 Context 생성
        SparkConf conf = new SparkConf().setAppName("Word Count").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 텍스트 파일에서 RDD 읽기
        JavaRDD<String> lines = sc.textFile("/Users/jdaddy/sampleData/wordCount.txt");

        // 각 라인을 단어로 분할하여 flatMap으로 각 단어 추출
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        // 각 단어에 대해 (word, 1) 형태의 Pair RDD 생성
        JavaPairRDD<String, Integer> wordCounts = words.mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey(Integer::sum);

        // 결과 출력
        wordCounts.foreach(tuple -> System.out.println(tuple._1() + " : " + tuple._2()));

        // Spark Context 종료
        sc.close();
    }
}
