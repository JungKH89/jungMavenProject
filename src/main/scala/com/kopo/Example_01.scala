package com.kopo
import org.apache.spark.sql.SparkSession
object Example_01 {
  def main(args: Array[String]): Unit = {

    val spark=SparkSession.builder().appName("kh").
      config("spark.master","local").
      getOrCreate()

    /// 끝자리 0인 수만 !
    var testArray=Array(22,33,50,70,90,100)

    var answer=testArray.filter(x=> {
      var data = x.toString
      var dataSize = data.size

      var lastChar = data.substring(dataSize - 1).toString
      lastChar.equalsIgnoreCase("0")
    })
  }

}
