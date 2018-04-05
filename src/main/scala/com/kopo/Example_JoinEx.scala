package com.kopo

import org.apache.spark.sql.SparkSession;
object Example_JoinEx {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("kh").
      config("spark.master", "local").
      getOrCreate()

    //oracle에서 kopo_channel_seasonality_new,kopo_region_mst 테이블을 불러와서
    //inner join, left join 수행하기!!!

    var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "kopo_channel_seasonality_new"

    val selloutData1= spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> selloutDb, "user" -> staticUser, "password" -> staticPw)).load

    selloutData1.createOrReplaceTempView("selloutTable1")


    var staticUrl2 = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
    var staticUser2 = "kopo"
    var staticPw2 = "kopo"
    var selloutDb2 = "kopo_region_mst"

    val selloutData2= spark.read.format("jdbc").
      options(Map("url" -> staticUrl2, "dbtable" -> selloutDb2, "user" -> staticUser2, "password" -> staticPw2)).load

    selloutData2.createOrReplaceTempView("selloutTable2")

    selloutData1.show()

    //innerJoin
    var innerjoinData= spark.sql("select a.regionid, a.product, a.yearweek, a.qty, b.regionname, b.regionid "+
      "from selloutTable1 a " +
      "inner join selloutTable2 b " +
      "on a.regionid = b.regionid")

    //leftJoin
    var leftjoinData= spark.sql("select a.regionid, a.product, a.yearweek, a.qty, b.regionname, b.regionid "+
      "from selloutTable1 a " +
      "left join selloutTable2 b " +
      "on a.regionid = b.regionid")

  }
}
