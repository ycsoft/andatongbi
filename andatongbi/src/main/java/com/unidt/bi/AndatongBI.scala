package com.unidt.bi

import com.unidt.helper.FraHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by admin on 2018/3/28.
  */
object AndatongBI {


  def sparkAdtBI(): Unit = {

    var conf = new SparkConf().setAppName("spark-hive-andatong")
    val spark = SparkSession.builder().config(conf)
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .enableHiveSupport()
      .getOrCreate()

    val hiveContext = new HiveContext(spark.sparkContext)

    hiveContext.sql("use andatong")
    hiveContext.sql("set hive.mapred.supports.subdirectories=true")
    hiveContext.sql("set mapreduce.input.fileinputformat.input.dir.recursive=true")
    hiveContext.sql("set hive.exec.dynamic.partition=true")
    hiveContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    val date = FraHelper.getDate()
    hiveContext.sql(f"select count(*) as pv from ods_andatong where p_dt=$date%s").collect().foreach(r => println(r))

  }

  def main(args: Array[String]): Unit = {
    while(true){
      sparkAdtBI()
      Thread.sleep(10000)
    }

  }


}
