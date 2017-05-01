/*
    Copyright (c) 2007-2014 Contributors as noted in the AUTHORS file

    This file is part of 0MQ.

    0MQ is free software; you can redistribute it and/or modify it under
    the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation; either version 3 of the License, or
    (at your option) any later version.

    0MQ is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

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

public class PullBind
{
    private MetricRegistry  registry;
    private String          seed;
    private CyclicBarrier   barrier;
    private Exchanger<Long> exchanger;

    public PullBind(MetricRegistry registry, String seed, CyclicBarrier barrier, Exchanger<Long> exchanger)
    {
        this.registry = registry;
        this.seed = seed;
        this.barrier = barrier;
        this.exchanger = exchanger;
    }

    public double[] run(String bindTo, long messageSize, long messageCount) throws Exception
    {
        Ctx ctx;
        SocketBase s;
        boolean rc;
        long i;
        Msg msg;
        long watch;
        long elapsed;
        long throughput;
        double megabits;

        long messagesReceived = 0;
        ctx = ZMQ.init(1);
        if (ctx == null) {
            printf("error in zmqInit\n");
            return null;
        }

        s = ZMQ.socket(ctx, ZMQ.ZMQ_PULL);
        if (s == null) {
            printf("error in zmq_socket\n");
        }

        //  Add your socket options here.
        //  For example ZMQ_RATE, ZMQ_RECOVERY_IVL and ZMQ_MCAST_LOOP for PGM.
        if (messageCount * messageSize > 10 * 1000 * 1000) {
            ZMQ.setSocketOption(s, ZMQ.ZMQ_RCVTIMEO, Long.valueOf(30 * 1000).intValue());
        }
        else {
            ZMQ.setSocketOption(s, ZMQ.ZMQ_RCVTIMEO, Long.valueOf(3 * 1000).intValue());
        }

        rc = ZMQ.bind(s, bindTo);
        if (!rc) {
            printf("error in zmq_bind: %s\n");
        }

        if (barrier != null) {
            barrier.await();
        }
        Long messagesSent = messageCount;
        if (exchanger != null) {
            //            messagesSent = exchanger.exchange(Long.valueOf(messageCount));
        }
        watch = ZMQ.startStopwatch();

        msg = ZMQ.recvMsg(s, 0);
        if (msg == null) {
            printf(" error in zmq_recvmsg: %s ", "first");
            double[] results = fillResults(watch, messageCount, messageSize, messagesReceived);
            ZMQ.close(s);

            ZMQ.term(ctx);
            return results;
        }
        ++messagesReceived;
        System.out.printf("-");

        for (i = 0; i != messageCount - 1; i++) {
            Context context = null;
            Context globalContext = null;
            if (registry != null && seed != null) {
                // for each msg
                Counter counter = registry.counter(seed + ".cnt." + i);
                counter.inc();
                Meter meter = registry.meter(seed + ".msg." + i);
                meter.mark();

                Timer timer = registry.timer(seed + ".timer.recv." + i);
                context = timer.time();

                timer = registry.timer(seed + ".timer.recv");
                globalContext = timer.time();
            }

            msg = ZMQ.recvMsg(s, 0);

            //          System.out.printf("xR%s", i);
            if (context != null) {
                context.stop();
                // for each msg
                Counter counter = registry.counter(seed + ".cnt." + i);
                counter.dec();
                Meter meter = registry.meter(seed + ".msg." + i);
                meter.mark();

                // globally
                counter = registry.counter(seed + ".cnt");
                counter.dec();
                meter = registry.meter(seed + ".recv.msg");
                meter.mark();

            }
            if (globalContext != null) {
                globalContext.stop();
            }

            if (msg == null) {
                printf(" error in zmq_recvmsg: %s ", i);
                double[] results = fillResults(watch, messageCount, messageSize, messagesReceived);
                ZMQ.close(s);

                ZMQ.term(ctx);
                return results;
            }
            ++messagesReceived;
            if (ZMQ.msgSize(msg) != messageSize) {
                printf("message of incorrect size received " + ZMQ.msgSize(msg) + "\n");
                return null;
            }
        }

        double[] results = fillResults(watch, messageCount, messageSize, messagesReceived);

        ZMQ.close(s);

        ZMQ.term(ctx);

        System.out.printf("R");
        return results;
    }

    private static double[] fillResults(long watch, long messageCount, long messageSize, long messagesReceived)
    {
        double[] results = new double[6];
        long elapsed = ZMQ.stopStopwatch(watch);
        if (elapsed == 0) {
            elapsed = 1;
        }

        System.out.printf("r");
        double seconds = elapsed / (1000 * 1000d);
        double throughput = (messageCount / seconds);
        double megabits = throughput * messageSize * 8 / 1000000;

        results[0] = seconds;
        results[1] = (int) messageSize;
        results[2] = (int) messageCount;
        results[3] = (int) throughput;
        results[4] = (int) megabits;
        results[5] = (int) messagesReceived;

        return results;
    }

    private static void printf(String str, Object... args)
    {
        System.out.printf(str, args);
    }

    private static int atoi(String string)
    {
        return Integer.valueOf(string);
    }

    private static long atol(String string)
    {
        return Long.valueOf(string);
    }

    private static void printf(String string)
    {
        System.out.println(string);
    }
}
