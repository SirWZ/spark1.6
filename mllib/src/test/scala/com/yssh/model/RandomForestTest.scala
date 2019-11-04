package com.yssh.model

import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.{SparkContext, SparkFunSuite}
import org.scalatest.{FunSuite, Tag}

class RandomForestTest extends FunSuite {
  test("加载随机森林") {
    val sc = new SparkContext("local[4]", "local")
    val modelPath = "/user/model/HXPBF2FT8131_RandomForestRegression_d67d0940-07b9-4f10-884a-3634722b3c3a"
    val model = RandomForestModel.load(sc, "hdfs://10.101.127.174" + modelPath)
    println(model.toString)
    sc.stop()
  }
}
