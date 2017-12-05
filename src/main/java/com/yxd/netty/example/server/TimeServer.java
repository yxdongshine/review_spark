/*
package com.yxd.netty.example.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.util.concurrent.DefaultThreadFactory;

public class TimeServer {
	
	public void bind(int port) throws Exception{
		EventLoopGroup bossGroup = new NioEventLoopGroup(2, new DefaultThreadFactory("server1", true));
		EventLoopGroup workerGroup = new NioEventLoopGroup(4, new DefaultThreadFactory("server2", true));
		
		try {
			ServerBootstrap b = new ServerBootstrap();
			b.group(bossGroup,workerGroup)
			.channel(NioServerSocketChannel.class)
			.childHandler(new ChildChannelHandler())
			.option(ChannelOption.SO_BACKLOG, 10240)//设置每行长度1024
			.childOption(ChannelOption.SO_KEEPALIVE, true);
			
			//绑定端口
			ChannelFuture f = b.bind(port).sync();
			
			//关闭端口
			f.channel().closeFuture().sync();
		} finally{
			//添加钩子 安全关闭线程池
			bossGroup.shutdownGracefully();
			workerGroup.shutdownGracefully();
		}
	}
	
	
	private class ChildChannelHandler extends ChannelInitializer<SocketChannel>{

		@Override
		protected void initChannel(SocketChannel paramC) throws Exception {
			// TODO Auto-generated method stub
			paramC.pipeline().addLast(new LineBasedFrameDecoder(10240));
			paramC.pipeline().addLast(new StringDecoder());
			paramC.pipeline().addLast(new TimeServerHandler());
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		new TimeServer().bind(50864);
	}

}
*/
