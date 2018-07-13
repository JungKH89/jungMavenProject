package com.kopo
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object Example_0419 {

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
    var qtyNo = rawDataColumns.indexOf("qty")

    //RDD변환
    var rawRdd = rawData.rdd

    var filteredRdd= rawRdd.filter(x=> {
      //boolean = true
      var checkValid=true
      // 찾기: yearweek인덱스로 주차정보만 인트타입으로 변환
      var weekValue = x.getString(yearweekNo).substring(4).toInt

      //비교한 후 주차정보가 53이상인 경우 레코드 삭제
      if(weekValue >= 53){
        checkValid=false
      }
      checkValid
    })


    //---------------------------filter--------------------------------
    //상품정보가 product 1,2 인 정보만 필터링
    //분석대상 제품군 등록
    var productAttay = Array("PRODUCT1","PRODUCT2")
    //세트 타입으로 변환
    var productSet = productAttay.toSet

    var resultRdd = filteredRdd.filter(x=>{
      var checkValid = false

      //데이터 특정 행의 product 컬럼인덱스를 활용하여 데이터 대입
      var productInfo = x.getString(productNo)

      if(productSet.contains(productInfo)){
        checkValid = true
      }

      //2번째 (근데 이거는 코드가 너무 지저분해져! 추천 ㄴㄴ)
      if((productInfo == "PRODUCT1") || (productInfo == "PRODUCT2") ){
        checkValid = true
      }
      checkValid
    })



    //resultRdd.first
    //resultRdd = (키정보, 지역정보, 상품정보, 연주차정보, 거래량 정보, 상품이름정보)
    //-----------------------------map--------------------------------
    //처리로직 : 거래량이 MAXVALUE 이상인건은 MAXVALUE로 치환
    var maxValue = 700000

    var mapRdd =resultRdd.map(x=>{
      //디버깅코드 : var x = mapRdd.filter(x=>{ x.getDouble(qtyNo) > 700000 }).first
      var org_qty = x.getDouble(qtyNo)
      var new_qty=org_qty
      if(new_qty > maxValue){new_qty = maxValue}

      //출력 row  키정보, 연주차정보, 거래량 정보_org, 거래량 정보_new
      //Row쓰려면 import해야함  import org.apache.spark.sql.{Row, SparkSession}
      Row( x.getString(keyNo),
        x.getString(yearweekNo),
        org_qty,
        new_qty
      )
    })


    //-----------------------Rdd -> DataFrame-------------------------
    //import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
    val finalResultDf = spark.createDataFrame(resultRdd,
      StructType(
        Seq(
          StructField("KEY", StringType),
          StructField("REGIONID", StringType),
          StructField("PRODUCT", StringType),
          StructField("YEARWEEK", StringType),
          StructField("VOLUME", DoubleType),
          StructField("PRODUCT_NAME", StringType))))

    finalResultDf.show(2)



  }


}
