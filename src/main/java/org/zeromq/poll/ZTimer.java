package org.zeromq.poll;

import zmq.util.Timers;

public final class ZTimer
{
    public static class Timer
    {
        private final Timers.Timer delegate;
        
        private Timer(Timers.Timer delegate)
        {
            this.delegate = delegate;
        }
    }

    public static interface Handler extends Timers.Handler
    {
    }

    private final Timers timer = new Timers();
    
    /**
     * Add timer to the set, timer repeats forever, or until cancel is called.
     * @param interval the interval of repetition in milliseconds.
     * @param handler the callback called at the expiration of the timer.
     * @param args the optional arguments for the handler.
     * @return an opaque handle for further cancel.
     */
    public Timer add(long interval, Handler handler, Object... args)
    {
        return new Timer(timer.add(interval, handler, args));
    }

    /**
     * Changes the interval of the timer.
     * This method is slow, cancelling existing and adding a new timer yield better performance.
     * @param timer the timer to change the interval to.
     * @return true if set, otherwise false.
     */
    public boolean setInterval(Timer timer, long interval)
    {
        return this.timer.setInterval(timer.delegate, interval);
    }

    /**
     * Reset the timer.
     * This method is slow, cancelling existing and adding a new timer yield better performance.
     * @param timer the timer to reset.
     * @return true if reset, otherwise false.
     */
    public boolean reset(Timer timer)
    {
        return this.timer.reset(timer.delegate);
    }

    /**
     * Cancel a timer.
     * @param timer the timer to cancel.
     * @return true if cancelled, otherwise false.
     */
    public boolean cancel(Timer timer)
    {
        return this.timer.cancel(timer.delegate);
    }

    /**
     * Returns the time in millisecond until the next timer.
     * @return the time in millisecond until the next timer.
     */
    public long timeout()
    {
        return timer.timeout();
    }

    /**
     * Execute the timers.
     * @return the number of timers triggered.
     */
    public int execute()
    {
        return timer.execute();
    }
}
