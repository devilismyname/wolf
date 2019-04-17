package cn.yunyingguanjia

import com.alibaba.fastjson.JSON
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object App {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    val data = sc.textFile(args(0))
    data.foreachPartition(it=>{
      it.map(m=>{
        JSON.parseObject(m)

      })



    })




  }
}
