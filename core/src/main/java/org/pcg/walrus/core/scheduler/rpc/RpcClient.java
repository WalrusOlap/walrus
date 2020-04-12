package org.pcg.walrus.core.scheduler.rpc;

import org.apache.mina.core.future.ConnectFuture;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.textline.TextLineCodecFactory;
import org.apache.mina.transport.socket.nio.NioSocketConnector;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;

/**
 * rpc client:
 */
public class RpcClient {

    // charset
    private static final Charset ENCODE_CHARSET = Charset.forName("UTF-8");

    private static int RPC_BUFFER_SIZE = 10 * 1024 * 1024;
    private static int RPC_TIME_OUT = 15;

    /**
     * send message
     */
    public static void sendMessage(InetAddress address, int port, RpcMessage msg) {
        NioSocketConnector nioSocketConnector = new NioSocketConnector();
        // config
        nioSocketConnector.setConnectTimeoutCheckInterval(RPC_TIME_OUT);
        nioSocketConnector.getSessionConfig().setReceiveBufferSize(RPC_BUFFER_SIZE);
        nioSocketConnector.getFilterChain().addLast("codec",
                new ProtocolCodecFilter(new TextLineCodecFactory(ENCODE_CHARSET)));

        // response handler
        nioSocketConnector.setHandler(new IoHandlerAdapter() {
            @Override
            public void messageReceived(IoSession session, Object message) throws Exception {

            }
        });
        // connect
        ConnectFuture connectFuture = nioSocketConnector.connect(new InetSocketAddress(address, port));
        connectFuture.awaitUninterruptibly();
        // send msg
        connectFuture.getSession().write(msg.getMessage());
        // close
        nioSocketConnector.dispose();
        connectFuture.getSession().getCloseFuture().awaitUninterruptibly();
    }
}
