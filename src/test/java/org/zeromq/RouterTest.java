package org.zeromq;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.zeromq.ZMQ.Poller;
import org.zeromq.ZMQ.Socket;

public class RouterTest
{
    @Test(timeout = 5000)
    public void testReconnectIssue589() throws InterruptedException
    {
        ExecutorService service = Executors.newFixedThreadPool(2);

        final ZMQ.Context ctx = ZMQ.context(1);
        assertThat(ctx, notNullValue());

        Socket control = ctx.socket(SocketType.PUB);
        assertThat(control, notNullValue());

        boolean rc = control.bind("inproc://control");
        assertThat(rc, is(true));

        // Configure client
        Socket client = ctx.socket(SocketType.ROUTER);
        assertThat(client, notNullValue());

        rc = client.setProbeRouter(true);
        assertThat(rc, is(true));

        // Configure server socket
        Socket server = ctx.socket(SocketType.ROUTER);
        assertThat(server, notNullValue());

        rc = server.setRouterMandatory(true);
        assertThat(rc, is(true));

        rc = server.setRouterHandover(true);
        assertThat(rc, is(true));

        rc = client.setIdentity("CLIENT".getBytes(ZMQ.CHARSET));
        assertThat(rc, is(true));

        rc = server.bind("tcp://*:*");
        assertThat(rc, is(true));

        final String ep = server.getLastEndpoint();

        // Run server
        service.submit(() -> {
            Socket controlled = ctx.socket(SocketType.SUB);
            assertThat(controlled, notNullValue());

            boolean rcs = controlled.subscribe(ZMQ.SUBSCRIPTION_ALL);
            assertThat(rcs, is(true));

            rcs = controlled.connect("inproc://control");
            assertThat(rcs, is(true));

            Poller poller = ctx.poller(2);
            poller.register(server, Poller.POLLIN);
            poller.register(controlled, Poller.POLLIN);

            while (true) {
                if (poller.poll(-1) <= 0) {
                    break;
                }
                if (poller.pollin(0)) {
                    // Expect peerId and a message
                    byte[] peerId = server.recv();
                    byte[] msg = server.recv();

                    // do some processing...
                    ZMQ.msleep(1000);

                    // 0-length indicates router-probe
                    if (msg.length == 0) {
                        System.out.println(String.format("Saying hi to PeerId: %s", Arrays.toString(peerId)));
                        if (server.sendMore(peerId)) {
                            server.send("hi!");
                            System.out.println("Done saying hi.");
                        }
                        else {
                            System.out.println("Not sending to an unavailable peer");
                        }
                    }
                    // else - DO ALL THE (app-specific) THINGS
                }
                if (poller.pollin(1)) {
                    break;
                }
            }
            controlled.close();
            poller.close();
        });

        service.submit(() -> {
            Socket controlled = ctx.socket(SocketType.SUB);
            assertThat(controlled, notNullValue());
            boolean rcs = controlled.subscribe(ZMQ.SUBSCRIPTION_ALL);
            assertThat(rcs, is(true));

            rcs = controlled.connect("inproc://control");
            assertThat(rcs, is(true));

            // Connect 1st time
            client.connect(ep);

            // Reconnect
            client.disconnect(ep);
            client.connect(ep);

            Poller poller = ctx.poller(2);
            poller.register(client, Poller.POLLIN);
            poller.register(controlled, Poller.POLLIN);

            while (true) {
                if (poller.poll(-1) <= 0) {
                    break;
                }
                if (poller.pollin(0)) {
                    byte[] peerId = client.recv();
                    String msg = client.recvStr();

                    System.out.println(String.format("PeerId: %s, Msg: %s", Arrays.toString(peerId), msg));
                }
                if (poller.pollin(1)) {
                    break;
                }
            }
            controlled.close();
        });

        ZMQ.sleep(2);
        control.send(ZMQ.PROXY_TERMINATE);

        service.shutdown();
        service.awaitTermination(10, TimeUnit.SECONDS);

        client.close();
        server.close();
        control.close();
        ctx.close();
    }
}
