package com.yssh.model

import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator
import org.apache.commons.compress.compressors.CompressorStreamFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.scalatest.concurrent.ScaledTimeSpans
import org.xerial.snappy.Snappy

class DemoTet extends FlatSpec with BeforeAndAfter with Matchers with ScaledTimeSpans {


  "测试 Snappy 压缩算法" should "" in {
    val str = "Hello snappy-java! Snappy-java is a JNI-based wrapper of Snappy, a fast compresser/decompresser."

    val bytes = Snappy.compress(str)

    val strUnCompress = Snappy.uncompressString(bytes)

    println(ObjectSizeCalculator.getObjectSize(str))

    println(ObjectSizeCalculator.getObjectSize(bytes))

    println(ObjectSizeCalculator.getObjectSize(strUnCompress))

  }

  "下载算法模型到本地" should "" in {

    val configuration = new Configuration()
    val fileSystem = FileSystem.newInstance(configuration)
    val modelPath = new Path("hdfs:10.101.127.174/user/model/HXPBF2FT8131_RandomForestRegression_d67d0940-07b9-4f10-884a-3634722b3c3a")
    val localPath = new Path(s"tmp/spark/models/${modelPath.getName}")
    fileSystem.copyToLocalFile(modelPath, localPath)

    fileSystem.close();

  }

}

