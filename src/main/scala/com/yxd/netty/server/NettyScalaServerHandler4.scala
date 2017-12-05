package com.yxd.netty.server

import com.yxd.Util.{DateUtil, SparkSqlUtil}
import com.yxd.spark_cache.SparkCache.Log
import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by YXD on 2017/12/5.
 */
class NettyScalaServerHandler4(sc:SparkContext,sqlContext:SQLContext) extends ChannelInboundHandlerAdapter{

  override def channelRead(ctx: ChannelHandlerContext, msg: AnyRef) {
    val body: String = msg.asInstanceOf[String]
    //System.out.println("server body: " + body )
    System.out.println("count:"+SparkSqlUtil.accumulation())
    //根据数据格式解析
    var result = grammaticalAnalysis(body)
    if(result != null
    &&result.trim.length>0){
      //返回业务端数据
      result = result+ System.getProperty("line.separator");
      val resp = Unpooled.copiedBuffer(result.getBytes());
      ctx.writeAndFlush(resp);
      //ctx.write(resp);
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
    cause.printStackTrace
    ctx.close
  }

  /**
   * 根据数据内容拆分
   *  如果含有"$@$"表示查询语句
   *  否则是存储数据
   * @param message
   */
  def grammaticalAnalysis(message: String): String = {
      var result = ""
      if(message.trim().length >= 0){
        //拆分
        val splitArr =  message.trim.split(SparkSqlUtil.DATA_SEPARATE)
        if(splitArr != null){
            if(splitArr.length == 1){
              //存储数据
              insert(splitArr(0))
            }else if(splitArr.length == 2){
              //表示查询数据
              result = select(splitArr(1))
            }
        }
      }
    result
  }


  /**
   * 数据清洗
   * @param body
   */
  def dataCleaning(body: String): String = {
    if(body != null
      && body.trim.length > 0){
      body.substring(body.indexOf(DateUtil.getDataPrefix()),body.length());
    }else{
      ""
    }
  }


    /**
   * 存储到spark 临时表中
   * @param message
   */
  def insert(message: String): Unit = {
    import sqlContext.implicits._
    //数据清理
    val cleanedData = dataCleaning(message)
    val data = Array(cleanedData)
    val bdf = sc
      .parallelize(data)
      .map(_.toString.split(" "))
      .map(
        l =>{
          if(0 == l.size){
            Log("","","","","","")
          }else if( 1 == l.size){
            Log(l(0),"","","","","")
          }else if( 2 == l.size){
            Log(l(0),l(1),"","","","")
          }else if( 3 == l.size){
            Log(l(0),l(1),l(2),"","","")
          }else if( 4 == l.size){
            Log(l(0),l(1),l(2),l(3),"","")
          }else if( 5 == l.size){
            Log(l(0),l(1),l(2),l(3),l(4),"")
          }else{
            Log(l(0),l(1),l(2),l(3),l(4),l(5))
          }
        }
      )
      .toDF()

    var df = sqlContext.sql("select * from "+SparkSqlUtil.TABLE_NAME)
    df = df.unionAll(bdf)
    df.registerTempTable(SparkSqlUtil.TABLE_NAME)
    df.cache()
  }

  /**
   * 业务端传入sql 查询spark
   * @param sql
   */
  def select(sql: String): String = {
    val df = sqlContext.sql(sql)
    val resultStr = df
      .collect()
      .foldLeft(""){
      (m, n) => m + n}
    resultStr
  }
}
