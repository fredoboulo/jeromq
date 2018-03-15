package org.zeromq;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Poller;
import org.zeromq.ZMQ.Socket;

import guide.ZHelper;

public class LoadBalancerBrokerTest
{
    private static final int NBR_CLIENTS         = 20;
    private static final int NBR_WORKERS         = 4;
    private static final int REQUESTS_PER_CLIENT = 10;

    private static final boolean VERBOSE = false;

    /**
     * Basic request-reply client using REQ socket
     */
    private static class ClientTask implements Runnable
    {
        private final int id;

        public ClientTask(int id)
        {
            this.id = id;
        }

        @Override
        public void run()
        {
            Context context = ZMQ.context(1);
            assertThat(context, notNullValue());

            //  Prepare our context and sockets
            Socket client = context.socket(ZMQ.REQ);
            assertThat(client, notNullValue());

            ZHelper.setId(client); //  Set a printable identity

            boolean rc = client.connect("ipc://frontend.ipc");
            assertThat(rc, is(true));

            for (int idx = 0; idx < REQUESTS_PER_CLIENT; ++idx) {
                //  Send request, get reply
                rc = client.send("HELLO");
                assertThat(rc, is(true));

                String reply = client.recvStr();
                assertThat(reply, is("OK"));
            }

            client.close();
            context.close();
            if (VERBOSE) {
                System.out.println(String.format("Closed Client %s", id));
            }
        }
    }

    /**
     * While this example runs in a single process, that is just to make
     * it easier to start and stop the example. Each thread has its own
     * context and conceptually acts as a separate process.
     * This is the worker task, using a REQ socket to do load-balancing.
     */
    private static class WorkerTask implements Runnable
    {
        private final int id;

        public WorkerTask(int id)
        {
            this.id = id;
        }

        @Override
        public void run()
        {
            Context context = ZMQ.context(1);
            assertThat(context, notNullValue());

            //  Prepare our context and sockets
            Socket worker = context.socket(ZMQ.REQ);
            assertThat(worker, notNullValue());

            ZHelper.setId(worker); //  Set a printable identity
            boolean rc = worker.connect("ipc://backend.ipc");
            assertThat(rc, is(true));

            //  Tell backend we're ready for work
            rc = worker.send("READY");
            assertThat(rc, is(true));

            while (!Thread.currentThread().isInterrupted()) {
                String address = worker.recvStr();
                String empty = worker.recvStr();
                assertThat(empty, is(""));

                //  Get request, send reply
                String request = worker.recvStr();
                if ("BYE".equals(request)) {
                    break;
                }

                rc = worker.sendMore(address);
                assertThat(rc, is(true));
                rc = worker.sendMore("");
                assertThat(rc, is(true));
                rc = worker.send("OK");
                assertThat(rc, is(true));
            }
            worker.close();
            context.term();
            if (VERBOSE) {
                System.out.println(String.format("Closed Worker %s", id));
            }
        }
    }

    /**
     * This is the main task. It starts the clients and workers, and then
     * routes requests between the two layers. Workers signal READY when
     * they start; after that we treat them as ready when they reply with
     * a response back to a client. The load-balancing data structure is
     * just a queue of next available workers.
     */
    @Test
    public void testLoadBalancerBroker() throws InterruptedException
    {
        Context context = ZMQ.context(1);
        assertThat(context, notNullValue());
        //  Prepare our context and sockets
        Socket frontend = context.socket(ZMQ.ROUTER);
        assertThat(frontend, notNullValue());
        Socket backend = context.socket(ZMQ.ROUTER);
        assertThat(backend, notNullValue());

        boolean rc = frontend.bind("ipc://frontend.ipc");
        assertThat(rc, is(true));
        rc = backend.bind("ipc://backend.ipc");
        assertThat(rc, is(true));

        int clientNbr;
        ExecutorService service = Executors.newFixedThreadPool(NBR_CLIENTS + NBR_WORKERS);

        for (clientNbr = 0; clientNbr < NBR_CLIENTS; clientNbr++) {
            service.submit(new ClientTask(clientNbr));
        }

        for (int workerNbr = 0; workerNbr < NBR_WORKERS; workerNbr++) {
            service.submit(new WorkerTask(workerNbr));
        }

        int requests = clientNbr * REQUESTS_PER_CLIENT;

        //  Here is the main loop for the least-recently-used queue. It has two
        //  sockets; a frontend for clients and a backend for workers. It polls
        //  the backend in all cases, and polls the frontend only when there are
        //  one or more workers ready. This is a neat way to use 0MQ's own queues
        //  to hold messages we're not ready to process yet. When we get a client
        //  reply, we pop the next available worker, and send the request to it,
        //  including the originating client identity. When a worker replies, we
        //  re-queue that worker, and we forward the reply to the original client,
        //  using the reply envelope.

        //  Queue of available workers
        Queue<String> workerQueue = new LinkedList<String>();

        while (!Thread.currentThread().isInterrupted()) {
            //  Initialize poll set
            Poller items = context.poller(2);

            //  Always poll for worker activity on backend
            items.register(backend, Poller.POLLIN);

            //  Poll front-end only if we have available workers
            if (workerQueue.size() > 0) {
                items.register(frontend, Poller.POLLIN);
            }

            if (items.poll() < 0) {
                break; //  Interrupted
            }

            //  Handle worker activity on backend
            if (items.pollin(0)) {
                //  Queue worker address for LRU routing
                workerQueue.add(backend.recvStr());

                //  Second frame is empty
                String empty = backend.recvStr();
                assert (empty.length() == 0);

                //  Third frame is READY or else a client reply address
                String clientAddr = backend.recvStr();

                //  If client reply, send rest back to frontend
                if (!"READY".equals(clientAddr)) {
                    empty = backend.recvStr();
                    assert (empty.length() == 0);

                    String reply = backend.recvStr();
                    frontend.sendMore(clientAddr);
                    frontend.sendMore("");
                    frontend.send(reply);

                    if (--requests == 0) {
                        for (String addr : workerQueue) {
                            backend.sendMore(addr);
                            backend.sendMore("");
                            backend.sendMore(clientAddr);
                            backend.sendMore("");
                            backend.send("BYE");
                        }
                        break;
                    }
                }
            }

            if (items.pollin(1)) {
                //  Now get next client request, route to LRU worker
                //  Client request is [address][empty][request]
                String clientAddr = frontend.recvStr();

                String empty = frontend.recvStr();
                assert (empty.length() == 0);

                String request = frontend.recvStr();

                String workerAddr = workerQueue.poll();

                backend.sendMore(workerAddr);
                backend.sendMore("");
                backend.sendMore(clientAddr);
                backend.sendMore("");
                backend.send(request);

            }
            items.close();
        }

        frontend.close();
        backend.close();
        context.term();

        service.shutdown();
        service.awaitTermination(1, TimeUnit.SECONDS);

        if (VERBOSE) {
            System.out.println("Closed Load Balancer");
        }
    }

    //    @Test
    public void testRepeated() throws InterruptedException
    {
        for (int idx = 0; idx < 1000; ++idx) {
            System.out.println(" " + idx);
            testLoadBalancerBroker();
        }
    }
}
