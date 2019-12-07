package org.zeromq.security;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

//  The Grasslands Pattern
//
//  The Classic ZeroMQ model, plain text with no protection at all.
public class GrassLandsTest
{
    @Test
    public void testGrassLands()
    {
        try (ZContext ctx = new ZContext(); ZContext ctxClient = new ZContext()) {
            //  Create and bind server socket
            ZMQ.Socket server = ctx.createSocket(SocketType.PUSH);

            boolean rc = server.bind("tcp://*:*");
            assertThat(rc, is(true));

            String endpoint = server.getLastEndpoint();

            //  Create and connect client socket
            ZMQ.Socket client = ctxClient.createSocket(SocketType.PULL);
            rc = client.connect(endpoint);
            assertThat(rc, is(true));

            //  Send a single message from server to client
            rc = server.send("Hello");
            assertThat(rc, is(true));

            String msg = client.recvStr();
            assertThat(msg, is("Hello"));

            client.close();
            server.close();
        }
    }
}
