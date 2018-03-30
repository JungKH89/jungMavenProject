package com.kopo
import org.apache.spark.sql.SparkSession
object Example_07 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("kh").
      config("spark.master", "local").
      getOrCreate()

    var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "kopo_channel_seasonality_ex"

    val selloutData= spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> selloutDb, "user" -> staticUser, "password" -> staticPw)).load

    selloutData.createOrReplaceTempView("selloutTable")
    selloutData.show()

    selloutData.schema

    selloutData.createTempView("maindata")

    var tet=spark.sql("select * from maindata")
    tet.show(1)

    var tet=spark.sql("select cast(qty as double) as qty from maindata")
    tet.show
    tet.schema
    var tet=spark.sql("select cast(qty as double) as qty2 from maindata")
    tet.show

  }
}