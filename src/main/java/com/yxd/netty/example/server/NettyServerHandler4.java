package com.yxd.netty.example.server;

import io.netty.buffer.ByteBuf;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
/**
 * Created by YXD on 2017/12/4.
 */
public class NettyServerHandler4 extends ChannelInboundHandlerAdapter{
    private static int counter;
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) { // (2)
        // Discard the received data silently.
        String body = (String)msg;
        System.out.println("server body: " + body + "  counter: "+ ++counter);

    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) { // (4)
        // Close the connection when an exception is raised.
        cause.printStackTrace();
        ctx.close();
    }
}
