package perf;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Exchanger;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;

import zmq.Ctx;
import zmq.Msg;
import zmq.SocketBase;
import zmq.ZMQ;

public class PushConnect
{
    private final MetricRegistry  registry;
    private final String          seed;
    private final CyclicBarrier   barrier;
    private final Exchanger<Long> exchanger;

    public PushConnect(MetricRegistry registry, String seed, CyclicBarrier barrier, Exchanger<Long> exchanger)
    {
        this.registry = registry;
        this.seed = seed;
        this.barrier = barrier;
        this.exchanger = exchanger;
    }

    public void run(String connectTo, int messageSize, long messageCount) throws Exception
    {
        Ctx ctx;
        SocketBase s;
        boolean rc;
        long i;
        Msg msg;

        ctx = ZMQ.init(1);
        if (ctx == null) {
            printf("error in zmqInit");
            return;
        }

        s = ZMQ.socket(ctx, ZMQ.ZMQ_PUSH);
        if (s == null) {
            printf("error in zmq_socket");
        }

        //  Add your socket options here.
        //  For example ZMQ_RATE, ZMQ_RECOVERY_IVL and ZMQ_MCAST_LOOP for PGM.

        ZMQ.setSocketOption(s, ZMQ.ZMQ_IMMEDIATE, false);
        rc = ZMQ.connect(s, connectTo);
        if (!rc) {
            printf("error in zmq_connect: %s\n");
            return;
        }
        if (barrier != null) {
            barrier.await();
        }

        for (i = 0; i < messageCount - 1; i++) {
            msg = ZMQ.msgInitWithSize(messageSize);
            if (msg == null) {
                printf("error in zmq_msg_init: %s\n");
                return;
            }
            if (messageSize > 10) {
                byte[] bytes = Long.toString(i).getBytes(ZMQ.CHARSET);
                for (int b : bytes) {
                    byte c = (byte) (b - 48);
                    msg.put(c);
                }
                msg.put((byte) 42);
                bytes = Long.toString(messageCount).getBytes(ZMQ.CHARSET);
                for (int b : bytes) {
                    byte c = (byte) (b - 48);
                    msg.put(c);
                }
                msg.put((byte) 42);
            }
            Context context = null;
            Context globalContext = null;
            if (registry != null && seed != null) {
                // for each msg
                Counter counter = registry.counter(seed + ".cnt." + i);
                counter.inc();
                Meter meter = registry.meter(seed + ".msg." + i);
                meter.mark();

                Timer timer = registry.timer(seed + ".timer.send." + i);
                context = timer.time();

                // globally
                counter = registry.counter(seed + ".cnt");
                counter.inc();
                meter = registry.meter(seed + ".send.msg");
                meter.mark();

                timer = registry.timer(seed + ".timer.send");
                globalContext = timer.time();
            }

            int n = ZMQ.sendMsg(s, msg, 0); //ZMQ.ZMQ_SNDMORE);
            //            System.out.printf("xS%s", i);
            if (context != null) {
                context.stop();
            }
            if (globalContext != null) {
                globalContext.stop();
            }
            if (n < 0) {
                printf("error in zmq_sendmsg: %s\n");
                return;
            }
        }
        msg = ZMQ.msgInitWithSize(messageSize);
        if (msg == null) {
            printf("error in zmq_msg_init: %s\n");
            return;
        }
        int n = ZMQ.sendMsg(s, msg, 0);
        if (n < 0) {
            printf("error in zmq_sendmsg: %s\n");
            return;
        }
        System.out.printf("s");

        if (exchanger != null) {
            exchanger.exchange(messageCount);
        }

        ZMQ.close(s);

        ZMQ.term(ctx);
        System.out.printf("S");
    }

    private static void printf(String string)
    {
        System.out.println(string);
    }
}
