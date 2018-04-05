package com.kopo

import org.apache.spark.sql.SparkSession;

object Example_Join {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("kh").
      config("spark.master", "local").
      getOrCreate()

    var dataPath="c:/spark/bin/data/"
    var mainData="kopo_channel_seasonality_ex.csv"
    var subData="KOPO_PRODUCT_MST.csv"

    //Dataframe
    var mainDataDf=spark.read.format("csv").option("header", "true").load(dataPath+mainData)
    var subDataDf=spark.read.format("csv").option("header","true").load(dataPath+subData)

    mainDataDf.createOrReplaceTempView("mainTable")
    subDataDf.createOrReplaceTempView("subTable")

    mainDataDf.show()

    //left join 예제
    var leftjoinData= spark.sql("select a.regionid, a.productgroup, b.productname, a.yearweek, a.qty "+
      "from mainTable a " +
      "left join subTable b " +
      "on a.productgroup = b.productid")

  }

}
