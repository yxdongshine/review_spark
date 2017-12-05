package com.yxd.netty.server

import com.yxd.Util.SparkSqlUtil
import com.yxd.spark_cache.SparkCache.Log
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by YXD on 2017/12/5.
 */
class NettyScalaServerHandler4(sc:SparkContext,sqlContext:SQLContext) extends ChannelInboundHandlerAdapter{

  private var counter: Int = 0

  override def channelRead(ctx: ChannelHandlerContext, msg: AnyRef) {
    val body: String = msg.asInstanceOf[String]
    System.out.println("server body: " + body + "  counter: " + ({
      counter += 1; counter
    }))

    //根据数据格式解析

  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
    cause.printStackTrace
    ctx.close
  }

  /**
   * 存储到spark 临时表中
   * @param body
   */
  def run(body: String): Unit = {
    import sqlContext.implicits._

    val data = Array(body)
    val bdf = sc
      .parallelize(data)
      .map(_.toString.split(" "))
      .map(
        l => Log(l(0),l(1),l(2),l(3),l(4),l(5))
      )
      .toDF()

    var df = sqlContext.tables(SparkSqlUtil.TABLE_NAME)
    df = df.unionAll(bdf)
    df.registerTempTable(SparkSqlUtil.TABLE_NAME)
    df.cache()
  }
}
