/*
package com.yxd.netty.example.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelHandlerAdapter;

import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.util.logging.Logger;

public class TimeClientHandler extends ChannelHandlerAdapter {
	private static final Logger logger = Logger
            .getLogger(TimeClientHandler.class.getName());

    private byte[] req;
	private int counter;

    public TimeClientHandler() {
        req = ("QUERY TIME ORDER"+ System.getProperty("line.separator")).getBytes();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
       ByteBuf message = null;
       for (int i = 0; i < 50; i++) {
    	   message = Unpooled.buffer(req.length);
    	   message.writeBytes(req);
           ctx.writeAndFlush(message);
       }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    	String body = (String)msg;
		System.out.println("client body: "+ body+"  counter: "+ ++counter);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
            throws Exception {
          // 释放资源
          logger.warning("Unexpected exception from downstream:"
                  + cause.getMessage());
          ctx.close();
    }
}
*/
