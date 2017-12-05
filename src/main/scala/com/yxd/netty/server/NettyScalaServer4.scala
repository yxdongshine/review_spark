package com.yxd.netty.server

import com.yxd.netty.example.server.{NettyServer4, NettyServerHandler4}
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.socket.SocketChannel
import io.netty.channel.{ChannelFuture, ChannelOption, ChannelInitializer, EventLoopGroup}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.LineBasedFrameDecoder
import io.netty.handler.codec.string.StringDecoder
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

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
  def run(sc:SparkContext,sqlContext:SQLContext) {
    val bossGroup: EventLoopGroup = new NioEventLoopGroup
    val workerGroup: EventLoopGroup = new NioEventLoopGroup
    try {
      val b: ServerBootstrap = new ServerBootstrap
      b.group(bossGroup, workerGroup)
        .channel(classOf[NioServerSocketChannel])
        .childHandler( new ChannelInitializer[SocketChannel] {
            @throws(classOf[Exception])
            def initChannel(ch: SocketChannel) {
              ch.pipeline.addLast(new LineBasedFrameDecoder(10240))
              ch.pipeline.addLast(new StringDecoder)
              ch.pipeline.addLast(new NettyScalaServerHandler4(sc,sqlContext))
            }
          })
        .option(ChannelOption.SO_BACKLOG, 128)
        .childOption(ChannelOption.SO_KEEPALIVE, true)

    val f: ChannelFuture = b.bind(port).sync
    f.channel.closeFuture.sync

    } finally {
      workerGroup.shutdownGracefully
      bossGroup.shutdownGracefully
    }
  }


}
