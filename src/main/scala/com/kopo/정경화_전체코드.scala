package com.kopo
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.SparkSession
import com.kopo.TestFunction
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object 정경화_전체코드 {
  val spark = SparkSession.builder().appName("kh").
    config("spark.master", "local").
    getOrCreate()


  var staticUrl = "jdbc:oracle:thin:@192.168.110.112:1521/orcl"
  var staticUser = "kopo"
  var staticPw = "kopo"
  var selloutDb = "kopo_channel_seasonality_new"
  val selloutData= spark.read.format("jdbc").
    options(Map("url" -> staticUrl, "dbtable" -> selloutDb, "user" -> staticUser, "password" -> staticPw)).load
  selloutData.createOrReplaceTempView("selloutTable")
  selloutData.show(2)


  var rawData=spark.sql("select " +
    "regionid, "+
    "product, " +
    "yearweek, " +
    "cast(qty as double) as qty, " +
    "(cast(qty as double) * 1.2) as qty_new " +
    "from selloutTable")

  var rawDataColumns = rawData.columns
  var regionidNo = rawDataColumns.indexOf("regionid")
  var productNo = rawDataColumns.indexOf("product")
  var yearweekNo = rawDataColumns.indexOf("yearweek")
  var qtyNo = rawDataColumns.indexOf("qty")
  var n_qtyNo = rawDataColumns.indexOf("qty_new")

  var rawRdd = rawData.rdd


  var productAttay = Array("PRODUCT1","PRODUCT2")
  var productSet = productAttay.toSet

  var rawExRdd = rawRdd.filter(x=>{
    var checkValid = false
    var yearInfo = x.getString(yearweekNo).substring(0,4).toInt
    var weekInfo = TestFunction.getWeekInfo(x.getString(yearweekNo))
    var productInfo = x.getString(productNo)

    if((yearInfo >= 2016) &&
      (weekInfo != 52) &&
      (productSet.contains(productInfo))){
      checkValid = true
    }
    checkValid
  })


  val finalResultDf = spark.createDataFrame(rawExRdd,
    StructType(
      Seq(
        StructField("regionid", StringType),
        StructField("product", StringType),
        StructField("yearweek", StringType),
        StructField("qty", DoubleType),
        StructField("qty_new", DoubleType))))

  finalResultDf.show(2)


  // 데이터베이스 주소 및 접속정보 설정
  var outputUrl = "jdbc:postgresql://192.168.110.111:5432/kopo"
  var outputUser = "kopo"
  var outputPw = "kopo"

  // 데이터 저장
  var prop = new java.util.Properties
  prop.setProperty("driver", "org.postgresql.Driver")
  prop.setProperty("user", outputUser)
  prop.setProperty("password", outputPw)
  var table = "kopo_st_result_jkh"
  //append
  finalResultDf.write.mode("overwrite").jdbc(outputUrl, table, prop)

}
