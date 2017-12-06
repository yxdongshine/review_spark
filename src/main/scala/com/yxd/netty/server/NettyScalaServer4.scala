package com.yxd.netty.server

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.socket.SocketChannel
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.LineBasedFrameDecoder
import io.netty.handler.codec.string.StringDecoder
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
 * Created by YXD on 2017/12/5.
 */
class NettyScalaServer4 {
    private var port: Int = 0

    def this(port: Int) {
      this()
      this.port = port
    }


  @throws(classOf[Exception])
  def run(sc:SparkContext,sqlContext:SQLContext,df:DataFrame) {
    val bossGroup: EventLoopGroup = new NioEventLoopGroup(1)
    //val workerGroup: EventLoopGroup = new NioEventLoopGroup
    try {
      val b: ServerBootstrap = new ServerBootstrap
      b.group(bossGroup)
        .channel(classOf[NioServerSocketChannel])
        .childHandler( new ChannelInitializer[SocketChannel] {
            @throws(classOf[Exception])
            def initChannel(ch: SocketChannel) {
              ch.pipeline.addLast(new LineBasedFrameDecoder(1024*1024*10))
              ch.pipeline.addLast(new StringDecoder)
              ch.pipeline.addLast(new NettyScalaServerHandler4(sc,sqlContext,df))
            }
          })
        //.option(ChannelOption.SO_BACKLOG, 128)
        //.childOption(ChannelOption.SO_KEEPALIVE, true)

    val f: ChannelFuture = b.bind(port).sync
    f.channel.closeFuture.sync

    } finally {
      //workerGroup.shutdownGracefully
      bossGroup.shutdownGracefully
    }
  }


}
