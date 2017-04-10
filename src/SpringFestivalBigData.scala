/**
  * Created by ASY on 2017/1/10.
  */

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import math._

object SpringFestivalBigData {

  def main(args: Array[String]) = {
    def Distance(lat_1:Float,lon_1:Float,lat_2:Float,lon_2:Float) = {
      val lat1 = lat_1 * 3.14/180
      val lon1 = lon_1 * 3.14/180
      val lat2 = lat_2 * 3.14/180
      val lon2 = lon_2 * 3.14/180
      val calLon = math.abs(lon2 - lon1)
      val calLat = math.abs(lat2 - lat1)
      val stepOne = math.pow(math.sin(calLat/2),2) + math.cos(lat1)*math.cos(lat2)*math.pow(math.sin(calLon/2),2)
      val stepTwo = 2 * math.asin(math.min(1, sqrt(stepOne)))
      val distance = 6367000*stepTwo//返回米
      distance
    }
    val conf = new SparkConf().setAppName("DPISearchData").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //val rawData = sc.textFile("hdfs://ns/data/hjpt/edm/oidd/oidd_position")
//    val rawOidd = sc.textFile("hdfs://ns/data/hjpt/edm/oidd/oidd_position/shanghai/20170104/10").map(line => line.split('|'))
//    D:\SpringFestivalBigData\part-r-00000_1
    //val rawOidd: RDD[Array[String]] = sc.textFile("hdfs://ns/data/hjpt/edm/oidd/oidd_position/*/20170109/10").map(line => line.split('|'))
    val rawOidd = sc.textFile("D:\\SpringFestivalBigData\\part-r-00000_1\\part-r-00000_1").map(line => line.split('|'))
    val oiddParase = rawOidd.filter(line => line.size > 8).map(line => (line(1), line(2), line(3), line(4), line(5))).filter(
      line => line._1 != "null" && line._2 != "null" && line._3 != "null" && line._4 !="null").map(line => (line._1, line._2, line._3, line._4.toFloat, line._5.toFloat))

    //val rawLonLat = sc.textFile("hdfs:///user/dm/asy/result").map(line => line.split(","))  //城市，景点名称，经度，纬度 大，小
    val rawLonLat = sc.textFile("C:/Users/ASY/Desktop/result").map(line => line.split(","))

    val scenicLocation = rawLonLat.map(line => ((line(3).toFloat,line(2).toFloat),(line(0),line(1)))).collect() //key:(纬度，经度),value:(城市，景点)
    val scenicLocationBC = sc.broadcast(scenicLocation)

    val scenicValuable = oiddParase.map {case(phone, areaID, time, lat_oidd, lon_oidd)=>
      {
        val scenicFilt: Array[((Float, Float), (String, String))] = scenicLocationBC.value.filter {case((lat_scenic, lon_scenic), (city, scenic))=>
//        lat_oidd - lat_scenic > -0.01 && lat_oidd - lat_scenic < 0.01 && lon_oidd - lon_scenic > -0.01 && lon_oidd - lon_scenic < 0.01
          Distance(lat_scenic, lon_scenic, lat_oidd, lon_oidd) < 1000
      }
        (phone, areaID, time, lat_oidd, lon_oidd, scenicFilt)
    }
    }
//    val oiddInScenic = scenicValuable.filter(line => !line._6.isEmpty)//保留在景点中的位置记录
//
//    val oiddInScenicDisc = oiddInScenic.map(line => (line._6(0)._2._1, line._6(0)._2._2, line._1, line._2)).distinct
//
//    val oiddInScenicDisc = oiddInScenic.map(line => ((line._1, line._2), line._6)).flatMapValues(line._1._1, line._1._2,line._2)
//
//    val oiddInScenicCount = oiddInScenicDisc.map(line => ((line._3._1, line._3._2),1)).reduceByKey(_+_)//每个景点的人数统计
//    val oiddInScenicSort = oiddInScenicCount.map(line => (line._1._1, line._1._2, line._2)).sortBy(line._2, false)
//
//    oiddInScenicSort.take(10)
//    oiddInScenicCount.saveAsTextFile("hdfs:///user/dm/asy/res20170111")
//    oiddInScenicSort.take(100)
  }
}
