package com.yssh.model

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.net.InetAddress

import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator
import org.apache.commons.codec.binary.Base64
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}
import org.scalatest.concurrent.ScaledTimeSpans
import org.scalatest.{Alerter, BeforeAndAfter, FlatSpec, FunSuite, Matchers, Tag}
import org.xerial.snappy.Snappy
import redis.clients.jedis.Jedis

class SparkTest extends FlatSpec with BeforeAndAfter with Matchers with ScaledTimeSpans {
  var sc: SparkContext = _

  import scala.util.Properties

  Properties.setProp("scala.time", "true")
  Logger.getRootLogger.setLevel(Level.ERROR)

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


  "模型保存到Redis" should "" in {

    def serialise(value: Any): Array[Byte] = {
      val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
      val oos = new ObjectOutputStream(stream)
      oos.writeObject(value)
      oos.close()
      stream.toByteArray
    }

    val modelPath = "tmp/spark/model/HXPBF2FT8131"
    val model: RandomForestModel = RandomForestModel.load(sc, modelPath)
    val modelSeri = serialise(model)
    val compress = Snappy.compress(modelSeri)
    val jedis = new Jedis("10.101.127.190", 6379)
    jedis.auth("123456")
    jedis.select(2)
    jedis.set("HXPBF2FT8131".getBytes(), compress)
    jedis.close()


  }


  "加载模型从Redis" should "" in {

    def deserialise[A](bytes: Array[Byte]): A = {
      val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
      val value = ois.readObject()
      ois.close()
      value.asInstanceOf[A]
    }

    val jedis = new Jedis("10.101.127.190", 6379)
    jedis.auth("123456")
    jedis.select(2)
    var start = System.currentTimeMillis()
    val compressModel = jedis.get("HXPBF2FT8131".getBytes())
    println(s"读取Redis耗时:${(System.currentTimeMillis() - start) / 1000.0} 秒")
    start = System.currentTimeMillis()
    val unCompress = Snappy.uncompress(compressModel)
    println(s"解压耗时:${(System.currentTimeMillis() - start) / 1000.0} 秒")

    start = System.currentTimeMillis()
    val model = deserialise[RandomForestModel](unCompress)
    println(s"反序列化耗时:${(System.currentTimeMillis() - start) / 1000.0} 秒")

    model.predict(Vectors.dense(Array(0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1)))


  }


  "模型序列化测试" should "" in {

    def deserialise[A](bytes: Array[Byte]): A = {
      val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
      val value = ois.readObject()
      ois.close()
      value.asInstanceOf[A]
    }

    def serialise(value: Any): Array[Byte] = {
      val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
      val oos = new ObjectOutputStream(stream)
      oos.writeObject(value)
      oos.close()
      stream.toByteArray
    }

    val modelPath = "tmp/spark/model/HXPBF2FT8131"
    var start = System.currentTimeMillis()
    val model: RandomForestModel = RandomForestModel.load(sc, modelPath)
    println(s"模型加载耗时:${(System.currentTimeMillis() - start) / 1000.0} 秒")
    println(s"模型占用内存:${ObjectSizeCalculator.getObjectSize(model)}")

    println("-------------------------------------------------------------")
    start = System.currentTimeMillis()
    val modelBytes = serialise(model)
    println(s"模型序列化耗时:${(System.currentTimeMillis() - start) / 1000.0} 秒")
    println(s"模型序列化后占用内存:${ObjectSizeCalculator.getObjectSize(modelBytes)}")


    println("-------------------------------------------------------------")
    start = System.currentTimeMillis()
    val randomForestModel: RandomForestModel = deserialise[RandomForestModel](modelBytes)
    println(s"模型反序列化耗时:${(System.currentTimeMillis() - start) / 1000.0} 秒")
    println(s"模型反序列化后占用内存:${ObjectSizeCalculator.getObjectSize(randomForestModel)}")


    println("-------------------------------------------------------------")
    start = System.currentTimeMillis()
    val compress = Snappy.compress(modelBytes)

    println(s"模型序列化后压缩耗时:${(System.currentTimeMillis() - start) / 1000.0} 秒")
    println(s"模型序列化 压缩后占用内存:${ObjectSizeCalculator.getObjectSize(compress)}")


    println("-------------------------------------------------------------")
    start = System.currentTimeMillis()
    val uncompress = Snappy.uncompress(compress)
    println(s"模型序列化后解压耗时:${(System.currentTimeMillis() - start) / 1000.0} 秒")
    println(s"模型序列化 解压后占用内存:${ObjectSizeCalculator.getObjectSize(uncompress)}")


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
