package com.kopo
import org.apache.spark.sql.SparkSession
object Example_0413 {

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

    var rawData = spark.sql("select " +
      "concat(a.regionid,'_',a.product) as keycol, " +
      "a.regionid as accountid, "+
      "a.product, " +
      "a.yearweek, "+
      "cast(a.qty as double) as qty, "+
      "b.product_name "+
      "from selloutTable a "+
      "left join mstTable b "+
      "on a.product = b.product_id")

    //column 인덱스 생성하기
    var rawDataColumns = rawData.columns
    var keyNo = rawDataColumns.indexOf("keycol")
    var accountidNo = rawDataColumns.indexOf("accountid")
    var productNo = rawDataColumns.indexOf("product")
    var yearweekNo = rawDataColumns.indexOf("yearweek")

    //RDD변환
    var rawRdd = rawData.rdd


    //불필요한 데이터 정제
    var rawExRdd1 = rawRdd.filter(x=> {
      //데이터 한줄씩 들어옴
      var checkValid=true

      //설정 부적합 로직
      if(x.getString(yearweekNo).length !=6){
        checkValid = false
      }
      checkValid
    })

    //A60 PRODUCT34 201402 4463
    //랜덤 디버깅 Case #1
    var x = rawRdd.first

    //디버깅 Case #2 (타겟팅 대상 선택)
    var rawExRdd2 = rawRdd.filter(x=>{
          var checkValid = false
          if((x.getString(accountidNo)== "A60") &&
            (x.getString(productNo) == "PRODUCT34") &&
            (x.getString(yearweekNo) == "201402")){
            checkValid = true
      }
      checkValid
    })
    rawExRdd2.count()
  }
}
