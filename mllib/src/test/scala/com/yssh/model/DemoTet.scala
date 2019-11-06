package com.yssh.model

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator
import org.apache.commons.compress.compressors.CompressorStreamFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.scalatest.concurrent.ScaledTimeSpans
import org.xerial.snappy.Snappy
import redis.clients.jedis.Jedis

class DemoTet extends FlatSpec with BeforeAndAfter with Matchers with ScaledTimeSpans {


  "测试 Snappy 压缩算法" should "" in {
    val str = "Hello snappy-java! Snappy-java is a JNI-based wrapper of Snappy, a fast compresser/decompresser."

    val bytes = Snappy.compress(str)

    val strUnCompress = Snappy.uncompressString(bytes)

    println(ObjectSizeCalculator.getObjectSize(str))

    println(ObjectSizeCalculator.getObjectSize(bytes))

    println(ObjectSizeCalculator.getObjectSize(strUnCompress))

  }

  "对象序列化测试" should "" in {


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

    val value1: Array[Byte] = serialise("aaaa")
    val value2: String = deserialise[String](value1)
    println(value2)

  }


  "下载算法模型到本地" should "" in {

    val configuration = new Configuration()
    val fileSystem = FileSystem.newInstance(configuration)
    val modelPath = new Path("hdfs:10.101.127.174/user/model/HXPBF2FT8131_RandomForestRegression_d67d0940-07b9-4f10-884a-3634722b3c3a")
    val localPath = new Path(s"tmp/spark/models/${modelPath.getName}")
    fileSystem.copyToLocalFile(modelPath, localPath)

    fileSystem.close();

  }

  "Redis 测试" should "" in {

    val jedis = new Jedis("10.101.127.190", 6379)
    jedis.auth("123456")
    jedis.select(2)



  }


}

