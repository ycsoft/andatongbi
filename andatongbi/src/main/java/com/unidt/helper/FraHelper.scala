package com.unidt.helper

import java.text.SimpleDateFormat
import java.util.Date

import com.unidt.hdfs.HdfsUtils

/**
  * Created by admin on 2018/3/23.
  */
object FraHelper {

  def getYear(): String = {
    var dateformater = new SimpleDateFormat("yyyy");
    var year = dateformater.format(new Date())
    year
  }
  def getYear(str: String): Int = {
    var year = 0
    if (str.length() > 10 ) {
      var vyear = str.split(" ")(0)
      year = vyear.split("-")(0).toInt
    }
    year
  }

  def getMonth():String = {
    var dateformater = new SimpleDateFormat("MM");
    var month = dateformater.format(new Date())
    month
  }
  def getMonth(str: String): Int = {
    var month = 0
    if (str.length() > 10 ) {
      var vyear = str.split(" ")(0)
      month = vyear.split("-")(1).toInt
    }
    month
  }

  def getDay(): String = {
    var dateformater = new SimpleDateFormat("dd");
    var day = dateformater.format(new Date())
    day
  }
  def getDay(str: String): Int = {
    var month = 0
    if (str.length() > 10 ) {
      var vyear = str.split(" ")(0)
      month = vyear.split("-")(2).toInt
    }
    month
  }

  def getHour(): String = {
    var dateformater = new SimpleDateFormat("HH");
    var hour = dateformater.format(new Date())
    hour
  }
  def getHour(str: String): Int = {
    var hour = 0
    if (str.length() > 10 ) {
      var vyear = str.split(" ")(1)
      hour = vyear.split(":")(0).toInt
    }
    hour
  }

  def getMinute(): String = {
    var dateformater = new SimpleDateFormat("mm");
    var minute = dateformater.format(new Date())
    minute
  }
  def getMinute(str: String): Int = {
    var minute = 0
    if (str.length() > 10 ) {
      var vyear = str.split(" ")(1)
      minute = vyear.split(":")(1).toInt
    }
    minute
  }

  def getSeconds(): String = {
    var dateformater = new SimpleDateFormat("ss");
    var sec = dateformater.format(new Date())
    sec
  }
  def getSeconds(str: String): Int = {
    var sec = 0
    if (str.length() > 10 ) {
      var vyear = str.split(" ")(1)
      sec = vyear.split(":")(2).toInt
    }
    sec
  }

  def get_time_stamp(str: String): Int = {
    var date: Date = null
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    date = format.parse(str)
    var ret = date.getTime / 1000
    ret.toInt
  }

  def getYearMonth(): String = {
    var dateformater = new SimpleDateFormat("yyyyMM");
    var ym = dateformater.format(new Date())
    ym
  }
  def getDate(): String = {
    var dateformater = new SimpleDateFormat("yyyyMMdd");
    var date = dateformater.format(new Date())
    date
  }

  /**
    * 该函数主要用于获取按日期分隔的目录
    * @return
    */
  def getPartitionByDate() : String = {
    val year = getYear()
    val yearmonth = getYearMonth()
    val date = getDate()
    return f"$year%s/$yearmonth%s/$date%s"
  }

  /**
    * 删除hdfs目录
    * @param path
    */
  def removeHdfsDir(path: String): Unit = {
    val dfstools = new HdfsUtils()
    dfstools.removeDir(path)
  }

  def writeHDFS(path: String,  content: String): Unit = {
    val dfs = new HdfsUtils()
    dfs.writeBuff(path, content)
  }

  def main(args: Array[String]): Unit = {
    println(getYear("2018-03-27 17:00:05"))
    println(getMonth("2018-03-27 17:00:05"))
    println(getDay("2018-03-27 17:00:05"))
    println(getHour("2018-03-27 17:00:05"))
    println(getMinute("2018-03-27 17:00:05"))
    println(getSeconds("2018-03-27 17:00:05"))
    println(get_time_stamp("2018-03-27 17:00:05"))
    println(get_time_stamp("2018-03-27 17:00:06"))
  }
}
