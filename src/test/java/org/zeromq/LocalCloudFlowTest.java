package org.zeromq;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.zeromq.ZMQ.Poller;
import org.zeromq.ZMQ.Socket;

//  Broker peering simulation (part 3)
//  Prototypes the full flow of status and tasks

public class LocalCloudFlowTest
{
    private static final boolean verbose = false;

    private static final int    NBR_CLIENTS  = 12;
    private static final int    NBR_WORKERS  = 4;
    private static final String WORKER_READY = "\001"; //  Signals worker is ready

    @Test
    public void testPrototypeLocalCloudFlowStandalone() throws InterruptedException
    {
        //  Our own name; in practice this would be configured per node
        final String self = "me";
        //  Our peers' names
        final List<String> peers = Arrays.asList("you", "other", "autre", "otro");

        assertPrototypeLocalCloudFlow(self, peers);
    }

    @Test
    public void testPrototypeLocalCloudFlowBinary() throws InterruptedException
    {
        assertPrototypeLocalCloudFlow("me", "you");
    }

    @Test
    public void testPrototypeLocalCloudFlowTriple() throws InterruptedException
    {
        assertPrototypeLocalCloudFlow("me", "you", "other");
    }

    @Test
    public void testPrototypeLocalCloudFlowQuatuor() throws InterruptedException
    {
        assertPrototypeLocalCloudFlow("me", "you", "other", "autre");
    }

    @Test
    public void testPrototypeLocalCloudFlowMultiple() throws InterruptedException
    {
        assertPrototypeLocalCloudFlow("me", "you", "other", "autre", "otro");
    }

    private void assertPrototypeLocalCloudFlow(final String... peers) throws InterruptedException
    {
        System.out.println("Local Flow Cloud with peers " + Arrays.toString(peers));
        ExecutorService service = Executors.newFixedThreadPool(peers.length);
        for (final String peer : peers) {
            final List<String> others = new ArrayList<>(Arrays.asList(peers));
            others.remove(peer);
            service.submit(new Callable<Object>()
            {
                @Override
                public Object call() throws Exception
                {
                    assertPrototypeLocalCloudFlow(peer, others);
                    return null;
                }
            });
        }
        service.shutdown();
        service.awaitTermination(60, TimeUnit.SECONDS);
    }

    //  This is the client task. It issues a burst of requests and then
    //  sleeps for a few seconds. This simulates sporadic activity; when
    //  a number of clients are active at once, the local workers should
    //  be overloaded. The client uses a REQ socket for requests and also
    //  pushes statistics to the monitor socket:
    private static class ClientTask implements Runnable
    {
        private final String self;
        private final int    id;

        public ClientTask(String self, int id)
        {
            this.self = self;
            this.id = id;
        }

        @Override
        public void run()
        {
            ZContext ctx = new ZContext();
            Socket client = ctx.createSocket(ZMQ.REQ);
            assertThat(client, notNullValue());

            boolean rc = client.connect(String.format("ipc://%s-localfe.ipc", self));
            assertThat(rc, is(true));

            Socket monitor = ctx.createSocket(ZMQ.PUSH);
            assertThat(monitor, notNullValue());

            rc = monitor.connect(String.format("ipc://%s-monitor.ipc", self));
            assertThat(rc, is(true));

            Random rand = new Random(System.nanoTime());

            Poller poller = ctx.createPoller(1);
            assertThat(poller, notNullValue());
            int ret = poller.register(client, Poller.POLLIN);
            assertThat(ret, is(0));

            int count = 3;
            while (count-- > 0) {
                ZMQ.msleep(rand.nextInt(5) * 100);

                int burst = rand.nextInt(15);

                while (burst > 0) {
                    String taskId = String.format("%04X", rand.nextInt(10000));
                    //  Send request, get reply
                    rc = client.send(self, ZMQ.SNDMORE);
                    assertThat(rc, is(true));
                    rc = client.send(taskId, 0);
                    assertThat(rc, is(true));

                    //  Wait max one second for a reply, then complain
                    ret = poller.poll(1000);
                    if (ret == -1) {
                        break; //  Interrupted
                    }

                    if (poller.pollin(0)) {
                        String reply = client.recvStr(0);
                        if (reply == null) {
                            break; //  Interrupted
                        }
                        //  Worker is supposed to answer us with our self
                        assertThat(reply, is(self));

                        reply = client.recvStr(0);
                        if (reply == null) {
                            break; //  Interrupted
                        }
                        //  Worker is supposed to answer us with our task id
                        assertThat(reply, is(taskId));

                        rc = monitor.send(String.format("%s-%s", self, reply), 0);
                        assertThat(rc, is(true));
                    }
                    else {
                        rc = monitor.send(String.format("E: CLIENT EXIT %s - lost task %s", self, taskId), 0);
                        assertThat(rc, is(true));

                        rc = monitor.send("EXIT");
                        assertThat(rc, is(true));

                        poller.close();
                        ctx.close();
                        return;
                    }
                    burst--;
                }
            }

            rc = client.send("EXIT");
            assertThat(rc, is(true));

            rc = monitor.send("EXIT");
            assertThat(rc, is(true));

            if (verbose) {
                System.out.printf("I: Exit client %s - %s%n", self, id);
            }
            poller.close();
            ctx.close();
        }
    }

    //  This is the worker task, which uses a REQ socket to plug into the LRU
    //  router. It's the same stub worker task you've seen in other examples:

    private static class WorkerTask implements Runnable
    {
        private final String self;
        private final int    id;

        public WorkerTask(String self, int id)
        {
            this.self = self;
            this.id = id;
        }

        @Override
        public void run()
        {
            Random rand = new Random(System.nanoTime());

            ZContext ctx = new ZContext();

            Socket worker = ctx.createSocket(ZMQ.REQ);
            assertThat(worker, notNullValue());

            boolean rc = worker.connect(String.format("ipc://%s-localbe.ipc", self));
            assertThat(rc, is(true));

            //  Tell broker we're ready for work
            ZFrame ready = new ZFrame(WORKER_READY);
            rc = ready.send(worker, 0);
            assertThat(rc, is(true));

            main: while (true) {
                //  Send request, get reply
                ZMsg msg = ZMsg.recvMsg(worker, 0);
                if (msg == null) {
                    break; //  Interrupted
                }

                ZMsg copy = msg.duplicate();
                String frame = copy.pollLast().toString();
                if ("EXIT".equals(frame)) {
                    break main;
                }
                frame = copy.pollLast().toString();
                if ("EXIT".equals(frame)) {
                    break main;
                }
                if (!self.equals(frame)) {
                    System.out.printf("Worker from '%s' received msg from cloud '%s'%n", self, frame);
                }
                //  Workers are busy for 0/100 milliseconds
                ZMQ.msleep(rand.nextInt(2) * 100);

                rc = msg.send(worker);
                assertThat(rc, is(true));
            }
            if (verbose) {
                System.out.printf("I: Exit worker %s - %s%n", self, id);
            }
            ctx.close();
        }
    }

    //  The main task begins by setting-up all its sockets. The local frontend
    //  talks to clients, and our local backend talks to workers. The cloud
    //  frontend talks to peer brokers as if they were clients, and the cloud
    //  backend talks to peer brokers as if they were workers. The state
    //  backend publishes regular state messages, and the state frontend
    //  subscribes to all state backends to collect these messages. Finally,
    //  we use a PULL monitor socket to collect printable messages from tasks:
    private void assertPrototypeLocalCloudFlow(String self, List<String> peers) throws InterruptedException
    {
        if (verbose) {
            System.out.printf("I: preparing broker at %s\n", self);
        }
        Random rand = new Random(System.nanoTime());

        ZContext ctx = new ZContext();

        //  Prepare local frontend and backend
        Socket localfe = ctx.createSocket(ZMQ.ROUTER);
        localfe.bind(String.format("ipc://%s-localfe.ipc", self));
        Socket localbe = ctx.createSocket(ZMQ.ROUTER);
        localbe.bind(String.format("ipc://%s-localbe.ipc", self));

        //  Bind cloud frontend to endpoint
        Socket cloudfe = ctx.createSocket(ZMQ.ROUTER);
        cloudfe.setIdentity(self.getBytes(ZMQ.CHARSET));
        cloudfe.bind(String.format("ipc://%s-cloud.ipc", self));

        //  Connect cloud backend to all peers
        Socket cloudbe = ctx.createSocket(ZMQ.ROUTER);
        cloudbe.setIdentity(self.getBytes(ZMQ.CHARSET));
        for (String peer : peers) {
            if (verbose) {
                System.out.printf("I: connecting to cloud forintend at '%s'\n", peer);
            }
            cloudbe.connect(String.format("ipc://%s-cloud.ipc", peer));
        }

        //  Bind state backend to endpoint
        Socket statebe = ctx.createSocket(ZMQ.PUB);
        statebe.bind(String.format("ipc://%s-state.ipc", self));

        //  Connect statefe to all peers
        Socket statefe = ctx.createSocket(ZMQ.SUB);
        statefe.subscribe(ZMQ.SUBSCRIPTION_ALL);
        for (String peer : peers) {
            if (verbose) {
                System.out.printf("I: connecting to state backend at '%s'\n", peer);
            }
            statefe.connect(String.format("ipc://%s-state.ipc", peer));
        }

        //  Prepare monitor socket
        Socket monitor = ctx.createSocket(ZMQ.PULL);
        monitor.bind(String.format("ipc://%s-monitor.ipc", self));

        ExecutorService service = Executors.newCachedThreadPool();
        //  Start local workers
        for (int workerNbr = 0; workerNbr < NBR_WORKERS; workerNbr++) {
            service.submit(new WorkerTask(self, workerNbr));
        }

        //  Start local clients
        int clientNbr;
        for (clientNbr = 0; clientNbr < NBR_CLIENTS; clientNbr++) {
            service.submit(new ClientTask(self, clientNbr));
        }

        //  Queue of available workers
        int localCapacity = 0;
        int cloudCapacity = 0;
        ArrayList<ZFrame> workers = new ArrayList<>();

        //  The main loop has two parts. First we poll workers and our two service
        //  sockets (statefe and monitor), in any case. If we have no ready workers,
        //  there's no point in looking at incoming requests. These can remain on
        //  their internal 0MQ queues:
        Poller primary = ctx.createPoller(4);
        primary.register(localbe, Poller.POLLIN);
        primary.register(cloudbe, Poller.POLLIN);
        primary.register(statefe, Poller.POLLIN);
        primary.register(monitor, Poller.POLLIN);

        Poller secondary = ctx.createPoller(2);
        secondary.register(localfe, Poller.POLLIN);
        secondary.register(cloudfe, Poller.POLLIN);

        while (true) {
            //  First, route any waiting replies from workers

            //  If we have no workers anyhow, wait indefinitely
            int rc = primary.poll(localCapacity > 0 ? 1000 : -1);
            if (rc == -1) {
                break; //  Interrupted
            }

            //  Track if capacity changes during this iteration
            int previous = localCapacity;

            //  Handle reply from local worker
            ZMsg msg = null;
            if (primary.pollin(0)) {
                msg = ZMsg.recvMsg(localbe);
                if (msg == null) {
                    break; //  Interrupted
                }
                ZFrame address = msg.unwrap();
                workers.add(address);
                localCapacity++;

                //  If it's READY, don't route the message any further
                ZFrame frame = msg.getFirst();
                if (new String(frame.getData(), ZMQ.CHARSET).equals(WORKER_READY)) {
                    msg.destroy();
                    msg = null;
                    continue;
                }
            }
            //  Or handle reply from peer broker
            else if (primary.pollin(1)) {
                msg = ZMsg.recvMsg(cloudbe);
                if (msg == null) {
                    break; //  Interrupted
                }
                //  We don't use peer broker address for anything
                ZFrame address = msg.unwrap();
                address.destroy();
            }
            //  Route reply to cloud if it's addressed to a broker
            if (msg != null) {
                for (String peer : peers) {
                    byte[] data = msg.getFirst().getData();
                    if (peer.equals(new String(data, ZMQ.CHARSET))) {
                        msg.send(cloudfe);
                        msg = null;
                    }
                }
            }
            //  Route reply to client if we still need to
            if (msg != null) {
                msg.send(localfe);
            }

            //  If we have input messages on our statefe or monitor sockets we
            //  can process these immediately:

            if (primary.pollin(2)) {
                String peer = statefe.recvStr();
                String status = statefe.recvStr();
                cloudCapacity = Integer.parseInt(status);
            }
            if (primary.pollin(3)) {
                String status = monitor.recvStr();
                if (verbose) {
                    System.out.println(status);
                }
                if ("EXIT".equals(status)) {
                    clientNbr--;
                    if (verbose) {
                        System.out.println("I: Clients " + clientNbr);
                    }
                    if (clientNbr == 0) {
                        break;
                    }
                }
            }

            //  Now we route as many client requests as we have worker capacity
            //  for. We may reroute requests from our local frontend, but not from //
            //  the cloud frontend. We reroute randomly now, just to test things
            //  out. In the next version we'll do this properly by calculating
            //  cloud capacity://

            while (localCapacity + cloudCapacity > 0) {
                rc = secondary.poll(0);

                assertThat(rc >= 0, is(true));

                if (secondary.pollin(0)) {
                    msg = ZMsg.recvMsg(localfe);
                }
                else if (localCapacity > 0 && secondary.pollin(1)) {
                    msg = ZMsg.recvMsg(cloudfe);
                }
                else {
                    break; //  No work, go back to backends
                }

                if (localCapacity > 0) {
                    ZFrame frame = workers.remove(0);
                    msg.wrap(frame);
                    msg.send(localbe);
                    localCapacity--;

                }
                else {
                    //  Route to random broker peer
                    final int randomPeer = rand.nextInt(peers.size());
                    msg.push(peers.get(randomPeer));
                    msg.send(cloudbe);
                }
            }

            //  We broadcast capacity messages to other peers; to reduce chatter
            //  we do this only if our capacity changed.

            if (localCapacity != previous) {
                //  We stick our own address onto the envelope
                statebe.sendMore(self);
                //  Broadcast new capacity
                statebe.send(String.format("%d", localCapacity), 0);
            }
        }
        //  When we're done, clean up properly
        while (workers.size() > 0) {
            ZFrame frame = workers.remove(0);
            frame.destroy();
        }

        service.shutdown();
        service.awaitTermination(4, TimeUnit.SECONDS);

        primary.close();
        secondary.close();
        ctx.close();
    }
}
