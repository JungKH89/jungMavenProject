package com.kopo
import org.apache.spark.sql.SparkSession
object Example_05 {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("kh").
      config("spark.master","local").
      getOrCreate()

    var staticUrl = "jdbc:oracle:thin:@192.168.110.10:1522/XE"
    var staticUser = "HAITEAM"
    var staticPw = "HA"
    var selloutDb = "KOPO_PRODUCT_VOLUME"


    val selloutDataFromOracle= spark.read.format("jdbc").
      options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load


    selloutDataFromOracle.createOrReplaceTempView("selloutTable")
    selloutDataFromOracle.show()


  }
}
