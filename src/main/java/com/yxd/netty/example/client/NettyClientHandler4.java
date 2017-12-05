package com.yxd.netty.example.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.Date;

/**
 * Created by YXD on 2017/12/4.
 */
public class NettyClientHandler4 extends ChannelInboundHandlerAdapter {

    private byte[] req;
    private int counter;

    public NettyClientHandler4() {
        req = ("QUERY TIME ORDER"+ System.getProperty("line.separator")).getBytes();
    }
      @Override
      public void channelRead(ChannelHandlerContext ctx, Object msg) {
            ByteBuf m = (ByteBuf) msg; // (1)
            try {
                long currentTimeMillis = (m.readUnsignedInt() - 2208988800L) * 1000L;
                System.out.println(new Date(currentTimeMillis));
                ctx.close();
            } finally {
                m.release();
            }
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
}
