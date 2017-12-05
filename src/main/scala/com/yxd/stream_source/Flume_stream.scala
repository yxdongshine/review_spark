package com.yxd.stream_source

import com.yxd.bean.Log
import org.apache.parquet.it.unimi.dsi.fastutil.Arrays
import org.apache.spark.SparkConf
import org.apache.spark.api.java.function.FlatMapFunction
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.flume.SparkFlumeEvent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.StreamingLinearRegressionWithSGD


/**
 * Created by YXD on 2017/11/21.
 *
 * 参考：https://www.cnblogs.com/hark0623/p/4172462.html
 */
object Flume_stream {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Flume_stream")
      .setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(60*1)) //指定批次运行间隔时间

    //创建 stream_flume
    val flumeStream = FlumeUtils.createStream(ssc, "192.168.163.180", 50864)
    //val flumeStream = FlumeUtils.createStream(ssc, "172.16.52.128", 50864)
    //flumeStream.count().map(cnt => "AAAAAA-Received " + cnt + " flume events." ).print()
    //数据逻辑处理
    //分界数字 8成数据训练 2层数据测试
    //val random_limit = 0.8

    val logRdd = flumeStream.map(log =>{
      val labelP = (new util.Random).nextInt(Int.MaxValue)+","+log.toString.hashCode+" "+(new util.Random).nextInt(Int.MaxValue)+" "+(new util.Random).nextInt(Int.MaxValue)
      LabeledPoint.parse(labelP)
      }
    )
   /* val logRdd = flumeStream.foreach(
        rdd => {
          val events: Array[SparkFlumeEvent] = rdd.collect()
          for (event <- events) {
            val sensorInfo = new String(event.event.getBody.array()) //单行记录\
            println(sensorInfo)
          }
          events.map(
            event =>{
              new String(event.event.getBody.array())
            }
          )
        }
    )
*/
    /*.map(
      arr => {
        if(arr.length >= 6){
          val day = arr(0)
          val date = arr(1)
          val r_no = arr(2)
          val service = arr(3)
          val log_level = arr(4)
          val data_info = arr(5)
          Log(day,date,r_no,service,log_level,data_info)
        }else{
          Log("1","1","1","1","1","1")
        }
      }
      )
    .map(
        log => {
          println(log.date)
          val splitSpaceLimlit = " "
          val splitCommaLimlit = ","
          var labelB = "0"
          val dateArr = log.date.split(splitCommaLimlit)
          if(dateArr.length >= 2){
            labelB = dateArr(1)
          }
          val logData = labelB+splitCommaLimlit+log.day.hashCode +splitSpaceLimlit+ log.date.hashCode +splitSpaceLimlit+ log.r_no.hashCode +splitSpaceLimlit+ log.service.hashCode +splitSpaceLimlit+ log.log_level.hashCode +splitSpaceLimlit+ log.data_info.hashCode
          val parts = logData.split(splitCommaLimlit)
          val label = java.lang.Double.parseDouble(parts(0))
          val features = Vectors.dense(parts(1).trim().split(splitSpaceLimlit)
            .map(java.lang.Double.parseDouble))
          LabeledPoint(label, features)
        }
      )*/
   /* .map(
        log =>{
          //训练和测试数据集合
          val trainData = new ArrayBuffer[LabeledPoint]()
          val testData = new ArrayBuffer[LabeledPoint]()
          val logData = log.date + log.date + log.r_no + log.service + log.log_level + log.data_info
          if((new util.Random).nextInt() <= random_limit){
            //作为训练数据
            trainData.+=( LabeledPoint.parse(logData))
          }else{
            testData.+=( LabeledPoint.parse(logData))
          }

          val data = new Array[ArrayBuffer[LabeledPoint]](2)
          data(0) = trainData
          data(1) = testData
          data
        }
      )*/

    //初始化权重
    val numFeatures = 3
    val model = new StreamingLinearRegressionWithSGD()
      .setInitialWeights(Vectors.zeros(numFeatures))

    //训练结果
    model.trainOn(logRdd)
    println("======================")
    model.predictOnValues(logRdd.map(lp => (lp.label, lp.features))).print()
    println("======================")
    //开始运行
    ssc.start()
    //计算完毕退出
    ssc.awaitTermination()
    //Thread.sleep(1000*60*10)
  }
}
