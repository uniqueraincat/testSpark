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

    val data: RDD[(String, Int)] = sc.textFile(hdfs_path)
      .flatMap{line =>
        val words: Array[(String, Int)] = line.trim()
          .split(" ")
          .map(word =>(word, 1))
        words
      }.reduceByKey{(a, b) =>
       a + b
    }

    val haha= data.collect()
      .foreach(x => "yy-log " +  println(x))


    sc.stop()

  }






  def main(args:Array[String])={

    val hdfs_path = "hdfs://keyboardcluster/ggyy/test"
    wordcount(hdfs_path:String)


  }
}
