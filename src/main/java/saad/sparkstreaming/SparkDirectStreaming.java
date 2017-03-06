package saad.sparkstreaming;


import java.util.regex.Pattern;
import java.io.*;
import java.util.*;
import scala.Tuple2;
import org.apache.spark.TaskContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.Durations;

/**
 * Consumes messages from one or more topics in Kafka.
 * Usage: SparkDirectStreaming.java <brokers> <topics>
 *   <brokers> is a list of one or more Kafka brokers
 *   <topics> isss a list of one or more kafka topics to consume from
 *
 *    
 */
public final class SparkDirectStreaming {
  private static final Pattern SPACE = Pattern.compile(" ");
  
  public static void main (String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println( "Usage: SparkDirectStreaming <brokers> <topics>/n" +
	     " <brokers> is a list of one or more Kafka brokers/n" +
	     " <topics> is a list of one or more Kafka topics to conusme from/n'n");
    }

    String brokers = args[0];
    String topics = args[1];
    //Spark Context
    SparkConf sparkConf = new SparkConf().setAppName("SparkDirectStreaming");
    //Sets batch interval of one second
    JavaStreamingContext jssc = new JavaStreamingContext (sparkConf , Durations.seconds(1));

    Collection<String> topiclist = Arrays.asList(topics.split(","));
    Map<String, Object> kafkaParams = new HashMap<>();
    kafkaParams.put("metadata.broker.list", brokers);
    kafkaParams.put("key.deserializer", StringDeserializer.class);
    kafkaParams.put("value.deserializer", StringDeserializer.class);
    kafkaParams.put("group.id", "JobEvensts");
    kafkaParams.put("auto.offset.reset", "latest");
    kafkaParams.put("enable.auto.commit", false);

    final JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
        jssc,
        LocationStrategies.PreferConsistent(),
        ConsumerStrategies.<String,String>Subscribe(topiclist, kafkaParams)
    );

    JavaDStream<String> lines = messages.map(new Function<ConsumerRecord<String,String>, String>() {
      @Override
      public String call(ConsumerRecord<String, String> kafkaRecord) throws Exception {
        return kafkaRecord.value();
      }
    });

    // Break every message into words and return list of words
    JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
      @Override
        public Iterator<String> call(String line) throws Exception {
          return Arrays.asList(line.split(" ")).iterator();
        }
    });

    // Take every word and return Tuple with (word,1)
    JavaPairDStream<String,Integer> wordMap = words.mapToPair(new PairFunction<String, String, Integer>() {
      @Override
        public Tuple2<String, Integer> call(String word) throws Exception {
          return new Tuple2<>(word,1);
        }
    });

    // Count occurance of each word
    JavaPairDStream<String,Integer> wordCounts = wordMap.reduceByKey(new Function2<Integer, Integer, Integer>() {
      @Override
        public Integer call(Integer i1, Integer i2) throws Exception {
          return i1+i2;
        }
    });


     wordCounts.print();
     jssc.start();
     jssc.awaitTermination();
  }

}
