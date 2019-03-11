import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row


object scalaCount {

  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf().setAppName("Word Count").setMaster("local[*]"))
    val textFile = sc.textFile("/Users/keatonkhonsari/shakes.txt");


    val counts = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    counts.collect

    // Save the output
    //counts.saveAsTextFile("/Users/keatonkhonsari/rdd_output")

    System.out.println("Total number of words: " + counts.count())

  }
}
