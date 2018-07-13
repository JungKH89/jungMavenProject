package com.kopo

object Def_Ex {
  def roundNum(num:Double, e:Int) :Double={
    var a=Math.round(num*Math.pow(10,e))/Math.pow(10,e)
    return a
  }


  //';'를 기점으로 앞, 뒤로 변수 나눠주기
  //2017;34를 입력받으면 2017과 34로 나누기
  def testDiv(str:String) :(Int, Int)={
    var inputValue = str
    var target = inputValue.split(";")
    var yearValue = target(0)
    var weekValue = target(1)
    return (yearValue.toInt, weekValue.toInt)
  }
}
