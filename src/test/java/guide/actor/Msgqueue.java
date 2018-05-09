package guide.actor;

import java.io.IOException;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZProxy;
import org.zeromq.ZProxy.Plug;
import org.zeromq.ZProxy.Proxy;

/**
* Simple message queuing broker
* Same as request-reply broker but using QUEUE device.
*/
public class Msgqueue
{
    public static void main(String[] args)
    {
        Proxy sockets = new ZProxy.Proxy.SimpleProxy()
        {
            @Override
            public Socket create(ZContext ctx, Plug place, Object... args)
            {
                if (place == Plug.FRONT) {
                    return ctx.createSocket(ZMQ.ROUTER);
                }
                if (place == Plug.BACK) {
                    return ctx.createSocket(ZMQ.DEALER);
                }
                return null;
            }

            @Override
            public boolean configure(Socket socket, Plug place, Object... args) throws IOException
            {
                if (place == Plug.FRONT) {
                    return socket.bind("tcp://*:5559");
                }
                if (place == Plug.BACK) {
                    return socket.bind("tcp://*:5560");
                }
                return true;
            }
        };
        ZProxy proxy = ZProxy.newZProxy(null, "msgqueue", sockets, "motdelafin");
        proxy.start(true);
        ZMQ.sleep(20);
        proxy.exit();
    }
}
