package zmq.util;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import zmq.ZMQ;
import zmq.api.ATimer;
import zmq.api.Draft;

@Draft
public final class Timers implements ATimer
{
    public static class Timer implements TimerHandle
    {
        private long           interval;
        private final Handler  handler;
        private final Object[] args;

        public Timer(long interval, Handler handler, Object... args)
        {
            this.interval = interval;
            this.handler = handler;
            this.args = args;
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + Arrays.hashCode(args);
            result = prime * result + ((handler == null) ? 0 : handler.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Timer other = (Timer) obj;
            if (handler == null) {
                if (other.handler != null) {
                    return false;
                }
            }
            else if (!handler.equals(other.handler)) {
                return false;
            }
            if (!Arrays.equals(args, other.args)) {
                return false;
            }
            return true;
        }
    }

    private final MultiMap<Long, Timer> timers          = new MultiMap<>();
    private final Set<Timer>            cancelledTimers = new HashSet<>();

    private long now()
    {
        return Clock.nowMS();
    }

    @Override
    public Timer add(long interval, Handler handler, Object... args)
    {
        if (handler == null) {
            return null;
        }
        final long when = now() + interval;
        final Timer timer = new Timer(interval, handler, args);
        final boolean rc = timers.insert(when, timer);
        assert (rc);
        return timer;
    }

    @Override
    public boolean setInterval(TimerHandle handle, long interval)
    {
        assert (handle instanceof Timer);
        Timer timer = (Timer) handle;
        if (timers.remove(timer)) {
            timer.interval = interval;
            return timers.insert(now() + interval, timer);
        }
        return false;
    }

    @Override
    public boolean reset(TimerHandle handle)
    {
        assert (handle instanceof Timer);
        Timer timer = (Timer) handle;

        if (timers.contains(timer)) {
            return timers.insert(now() + timer.interval, timer);
        }
        return false;
    }

    @Override
    public boolean cancel(TimerHandle handle)
    {
        assert (handle instanceof Timer);
        Timer timer = (Timer) handle;

        if (timers.contains(timer)) {
            return cancelledTimers.add(timer);
        }
        return false;
    }

    @Override
    public long timeout()
    {
        final long now = now();
        for (Entry<Timer, Long> entry : timers.entries()) {
            final Timer timer = entry.getKey();
            final Long timeout = entry.getValue();

            if (!cancelledTimers.remove(timer)) {
                //  Live timer, lets return the timeout
                if (timeout > now) {
                    return timeout - now;
                }
                else {
                    return 0;
                }
            }

            //  Remove it from the list of active timers.
            timers.remove(timeout, timer);
        }
        //  Wait forever as no timers are alive
        return -1;
    }

    @Override
    public int execute()
    {
        int executed = 0;
        final long now = now();
        for (Entry<Timer, Long> entry : timers.entries()) {
            final Timer timer = entry.getKey();
            final Long timeout = entry.getValue();

            //  Dead timer, lets remove it and continue
            if (cancelledTimers.remove(timer)) {
                //  Remove it from the list of active timers.
                timers.remove(timeout, timer);
                continue;
            }
            //  Map is ordered, if we have to wait for current timer we can stop.
            if (timeout > now) {
                break;
            }

            timers.insert(now + timer.interval, timer);

            timer.handler.time(timer.args);
            ++executed;
        }
        return executed;
    }

    /**
     * Sleeps until at least one timer can be executed and execute the timers.
     *
     * @return the number of timers triggered.
     */
    public int sleepAndExecute()
    {
        long timeout = timeout();
        while (timeout > 0) {
            ZMQ.msleep(timeout);
            timeout = timeout();
        }
        return execute();
    }
}
