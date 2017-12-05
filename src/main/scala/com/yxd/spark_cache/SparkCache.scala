package com.yxd.spark_cache

//import com.yxd.netty.example.server.TimeServer

import com.yxd.Util.SparkSqlUtil
import com.yxd.netty.example.server.NettyServer4
import com.yxd.netty.server.NettyScalaServer4
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by YXD on 2017/12/4.
 */
object SparkCache {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("SparkCache")
      .setMaster("local[*]")
    //.setMaster("spark://hadoop1:7070")
    val sc = new SparkContext(conf)
    //初始化sparksql
    val sqlContext = SQLContext.getOrCreate(sc)

    //创建表
    initTable(sc,sqlContext)
    //检查是否创建成功
    //showRecord(sc,sqlContext)
    //启动服务器
    //startNettyServer(50864)
    new NettyScalaServer4(50864)
    .run(sc,sqlContext)
  }

  case class Log (
                   day:String,
                   date:String,
                   r_no:String,
                   service:String,
                   log_level:String,
                   data_info:String
                   )

  /**
   * 初始化表
   * @param sc
   * @param sqlContext
   */
  def initTable(sc:SparkContext,sqlContext:SQLContext): Unit = {
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._
    //初始化数据建表
    // Create an RDD of Person objects and register it as a table.
    val logDf: DataFrame = sc
      .textFile("./src/main/resources/init_spark_cache_rdd.txt")
      .map(_.split(" "))
      .map(
        l => Log(l(0),l(1),l(2),l(3),l(4),l(5))
      )
      .toDF()

    logDf.registerTempTable(SparkSqlUtil.TABLE_NAME)
    logDf.cache()

  }

  /**
   *  //启动netty 服务器
   */
  def showRecord(sc:SparkContext,sqlContext:SQLContext): Unit = {
    import sqlContext.implicits._

    val data = Array("00 11 22 33 44 55")
    val df1 = sc
      .parallelize(data)
      .map(_.toString.split(" "))
      .map(
        l => Log(l(0),l(1),l(2),l(3),l(4),l(5))
      )
      .toDF()

    var df = sqlContext.sql("select * from weblogs")
    df = df.unionAll(df1)
    df.show()
    println("===========================")
    val df2 = sqlContext.sql("select * from weblogs")
    df2.show()
    df.registerTempTable("weblogs")
    df.cache()
    println("========5555555555555===================")
    val df3 = sqlContext.sql("select * from weblogs")
    df3.show()
  }


  /**
   *  //启动netty 服务器
   * @param port
   */
  def startNettyServer(port:Int): Unit = {
    val ns = new NettyServer4(port)
    ns.run()
  }



}
