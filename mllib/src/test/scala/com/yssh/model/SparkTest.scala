package com.yssh.model

import java.net.InetAddress

import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}
import org.scalatest.concurrent.ScaledTimeSpans
import org.scalatest.{Alerter, BeforeAndAfter, FlatSpec, FunSuite, Matchers, Tag}

class SparkTest extends FlatSpec with BeforeAndAfter with Matchers with ScaledTimeSpans {
  var sc: SparkContext = _

  import scala.util.Properties

  Properties.setProp("scala.time", "true")


  before {
    val conf: SparkConf = new SparkConf()
      .setAppName("Test")
      .setMaster("local[2]")
      .set("spark.local.dir", "tmp/spark")

    sc = new SparkContext(conf)

  }

  "时间统计" should " time" in {

    val unit = ObjectSizeCalculator.getObjectSize("")
    println(unit)

  }


  "随机森林模型加载测试" should " should be empty on crate" in {

    val modelPath = "/user/model/HXPBF2FT8131_RandomForestRegression_d67d0940-07b9-4f10-884a-3634722b3c3a"
    val model: RandomForestModel = RandomForestModel.load(sc, "hdfs://10.101.127.174" + modelPath)
    val unit = ObjectSizeCalculator.getObjectSize(model)
    println(unit)
        model.save(sc, "/home/sun/Intellijs/spark1.6/mllib/tmp/spark/model/HXPBF2FT8131")
  }




  after {
    sc.stop()
  }


}
