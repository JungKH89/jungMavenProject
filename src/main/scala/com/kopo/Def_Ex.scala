package com.kopo

object Def_Ex {
  def roundNum(num:Double, e:Int) :Double={
    var a=Math.round(num*Math.pow(10,e))/Math.pow(10,e)
    return a
  }
}
