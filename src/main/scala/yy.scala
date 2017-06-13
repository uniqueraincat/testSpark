/**
  * Created by Administrator on 2017/6/13.
  */

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
object yy {


  def wordcount(hdfs_path:String)={

    val conf = new SparkConf()
    val sc = new SparkContext(conf)


    val ss: RDD[String] = sc.parallelize(Array("123 456 a b c", "yy gg tt rr ss tt"))
    val data = ss
      .flatMap{line =>
        val words: Array[(String, Int)] = line.trim()
          .split(" ")
          .map(word =>(word, 1))
        words
      }.reduceByKey{(a, b) =>
       a + b
    }

    println("yy-log " + data.count())


    data.collect()
      .foreach(x => "yy-log " +  println(x))


    sc.stop()

  }






  def main(args:Array[String])={

    val hdfs_path = "hdfs://keyboardcluster/ggyy/test"
    wordcount(hdfs_path:String)


  }
}
