package com.kopo
import org.apache.spark.sql.SparkSession
object Example_0412 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("kh").
      config("spark.master", "local").
      getOrCreate()

    //oracle connection
    var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "kopo_channel_seasonality_new"
    var productNameDb= "kopo_product_mst"

    val selloutData= spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> selloutDb, "user" -> staticUser, "password" -> staticPw)).load

    val productMasterDf= spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> productNameDb, "user" -> staticUser, "password" -> staticPw)).load

    selloutData.createOrReplaceTempView("selloutTable")
    productMasterDf.createOrReplaceTempView("mstTable")
    selloutData.show()
    productMasterDf.show()

    //불러온 테이블 left join
    //concat & cast
    var middleResult = spark.sql("select " +
      "concat(a.regionid,'_',a.product) as keycol, " +
      "a.regionid, "+
      "a.product, " +
      "a.yearweek, "+
      "cast(a.qty as double) as qty, "+
      "b.product_name "+
      "from selloutTable a "+
      "left join mstTable b "+
      "on a.product = b.product_id")

    middleResult.show()
  }
}
