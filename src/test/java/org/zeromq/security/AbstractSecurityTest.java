package org.zeromq.security;

import org.zeromq.SocketType;
import org.zeromq.ZAuth;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import zmq.util.function.Supplier;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

abstract class AbstractSecurityTest
{
    interface Configurer
    {
        void configure(ZMQ.Socket client, AbstractServer server) throws IOException;
    }

    abstract static class AbstractServer
    {
        private final ZContext ctx;
        final ZAuth auth;
        final ZMQ.Socket socket;

        AbstractServer()
        {
            ctx = new ZContext();
            //  Start an authentication engine for this context. This engine
            //  allows or denies incoming connections (talking to the libzmq
            //  core over a protocol called ZAP).
            auth = new ZAuth(ctx, "auth");

            //  Get some indication of what the authenticator is deciding
            auth.verbose(true);

            //  Create server socket
            socket = ctx.createSocket(SocketType.PUSH);
        }

        abstract void configure();

        final void start()
        {
            configure();

            boolean rc = socket.bind("tcp://*:*");
            assertThat(rc, is(true));
        }

        final void close() throws IOException
        {
            socket.close();
            auth.close();
            ctx.close();
        }
    }

    final void assertSuccess(Supplier<AbstractServer> serverSupplier, Configurer clientConfigurer) throws IOException
    {
        try (ZContext ctx = new ZContext()) {
            AbstractServer server = serverSupplier.get();
            server.start();

            //  Create client socket
            ZMQ.Socket client = ctx.createSocket(SocketType.PULL);
            clientConfigurer.configure(client, server);
            assertSuccess(client, server);
        }
    }

    final void assertAccessDenied(Supplier<AbstractServer> serverSupplier, Configurer clientConfigurer) throws IOException
    {
        try (ZContext ctx = new ZContext()) {
            AbstractServer server = serverSupplier.get();
            server.start();

            //  Create client socket
            ZMQ.Socket client = ctx.createSocket(SocketType.PULL);
            clientConfigurer.configure(client, server);
            assertAccessDenied(client, server);
        }
    }

    private void assertSuccess(ZMQ.Socket client, AbstractServer server) throws IOException
    {
        boolean rc = client.connect(server.socket.getLastEndpoint());
        assertThat(rc, is(true));

        //  Send a single message from server to client
        rc = server.socket.send("Hello");
        assertThat(rc, is(true));

        String msg = client.recvStr();
        assertThat(msg, is("Hello"));

        client.close();
        server.close();
    }

    private void assertAccessDenied(ZMQ.Socket client, AbstractServer server) throws IOException
    {
        boolean rc = client.connect(server.socket.getLastEndpoint());
        assertThat(rc, is(true));

        //  Send a single message from server to client
        rc = server.socket.send("Hello");
        assertThat(rc, is(true));

        client.setReceiveTimeOut(100);
        String msg = client.recvStr();
        assertThat(msg, nullValue());

        client.close();
        server.close();
    }
}
