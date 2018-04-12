package com.kopo
import org.apache.spark.sql.SparkSession
object Example_04 {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("kh").
      config("spark.master","local").
      getOrCreate()

    //sqlserver접속
    var staticUrl = "jdbc:sqlserver://192.168.110.70:1433;databaseName=kopo"
    var staticUser = "haiteam"
    var staticPw = "haiteam"
    var selloutDb = "kopo_product_volume"

    val selloutDataFromSqlserver= spark.read.format("jdbc").
      options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load

    selloutDataFromSqlserver.registerTempTable("selloutTable")
    selloutDataFromSqlserver.show(1)

  }
}
