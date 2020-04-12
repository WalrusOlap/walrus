package org.pcg.walrus.core.scheduler.rpc;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;

import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.textline.TextLineCodecFactory;

import org.apache.mina.transport.socket.nio.NioSocketAcceptor;

import org.pcg.walrus.core.scheduler.TaskMonitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * walrus runner rpc server
 */
public class RpcServer {

    private static final Logger log = LoggerFactory.getLogger(RpcServer.class);

    // charset
    private static final Charset ENCODE_CHARSET = Charset.forName("UTF-8");

    private static int RPC_BUFFER_SIZE = 10 * 1024 * 1024;

    private IoAcceptor ioAcceptor;

    public RpcServer() {
        ioAcceptor = new NioSocketAcceptor();
    }

    /**
     * start rpc server
     */
    public void start(InetAddress address, int port, TaskMonitor monitor) {
        // config
        ioAcceptor.getSessionConfig().setReadBufferSize(RPC_BUFFER_SIZE);
        ioAcceptor.getFilterChain().addLast("codec",
                new ProtocolCodecFilter(new TextLineCodecFactory(ENCODE_CHARSET)));
        // handler
        ioAcceptor.setHandler(new IoHandlerAdapter() {
            @Override
            public void sessionCreated(IoSession session) throws Exception {

            }

            @Override
            public void sessionOpened(IoSession session) throws Exception {

            }

            @Override
            public void exceptionCaught(IoSession session, Throwable e) throws Exception {

            }

            @Override
            public void messageReceived(IoSession session, Object message) throws Exception {
                RpcMessage msg = RpcMessage.parse(message.toString());
                long task = msg.taskId;
                log.info(String.format("messageReceived: %s -> %s", msg.method, task));
                switch (msg.method) {
                    case RpcMessage.METHOD_RUN:
                        monitor.run(task);
                        break;
                    case RpcMessage.METHOD_KILL:
                        monitor.kill(task);
                        break;
                    default:
                        break;
                }
            }
        });
        new Thread() {
            @Override
            public void run() {
                try{
                    // start server
                    ioAcceptor.bind(new InetSocketAddress(address, port));
                    log.info(String.format("rpc server start on %s:%s", address.getHostName(), port));
                } catch (Exception e) {
                    log.error("rpc server start error: " + e.getMessage());
                }
            }
        }.start();
    }

    /**
     * stop server
     */
    public void stop() {
        ioAcceptor.unbind();
    }

}
