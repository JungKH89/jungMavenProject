package com.kopo
import org.apache.spark.sql.SparkSession
object Example_03 {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("kh").
      config("spark.master","local").
      getOrCreate()

    //mysql접속
    var  staticUrl = "jdbc:mysql://192.168.110.112:3306/kopo"
    var  staticUser = "root"
    var  staticPw = "P@ssw0rd"
    var selloutDb = "KOPO_PRODUCT_VOLUME"

    val selloutDataFromMysql= spark.read.format("jdbc").
      options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load

    selloutDataFromMysql.createOrReplaceTempView("selloutTable")
    selloutDataFromMysql.show(2)
  }
}
