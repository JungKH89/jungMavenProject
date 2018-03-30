package com.kopo
import org.apache.spark.sql.SparkSession
object Example_06 {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("kh").
      config("spark.master","local").
      getOrCreate()

    //postgresql에서 kopo_batch_season_mpara 테이블 불러오기 예제
    var staticUrl="jdbc:postgresql://192.168.110.111:5432/kopo"
    var staticUser="kopo"
    var staticPw="kopo"
    var selloutDb="kopo_batch_season_mpara"

    var pgParamData=spark.read.format("jdbc")
      .options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load

    pgParamData.createOrReplaceTempView("selloutTable")

    pgParamData.show()

    //postgresql에서 불러온 kopo_batch_season_mpara를 오라클에다가 다시 저장하기
    var outputUrl= "jdbc:oracle:thin:@192.168.110.10:1522/XE"
    var outputUser = "HAITEAM"
    var outputPw = "HA"

    var prop=new java.util.Properties
    prop.setProperty("driver","oracle.jdbc.OracleDriver")
    prop.setProperty("user",outputUser)
    prop.setProperty("password",outputPw)
    var table = "kopo_batch_season_mpara"

    pgParamData.write.mode("overwrite").jdbc(outputUrl,table,prop)
  }
}
