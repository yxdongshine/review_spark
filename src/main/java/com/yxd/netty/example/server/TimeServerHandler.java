/*
package com.yxd.netty.example.server;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;

public class TimeServerHandler extends ChannelHandlerAdapter{

	private int counter;
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg)
			throws Exception {
		// TODO Auto-generated method stub
		*/
/*ByteBuffer bbb = (ByteBuffer)msg;
	    CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();
		CharBuffer charBuffer = decoder.decode(bbb);
		String body = charBuffer.toString();*//*

		String body = (String)msg;
		System.out.println("server body: "+body+"  counter: "+ ++counter);
		String currentTime = "QUERY TIME ORDER".equalsIgnoreCase(body)?
				new java.util.Date(System.currentTimeMillis()).toString():"BADY ORDER";
		currentTime = currentTime+ System.getProperty("line.separator");
		ByteBuf resp = Unpooled.copiedBuffer(currentTime.getBytes());
		//ctx.writeAndFlush(resp);
		ctx.write(resp);
	}
	
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
			throws Exception {
		// TODO Auto-generated method stub
		ctx.close();
	}
}
*/
