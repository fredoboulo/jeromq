package guide.actor;

import java.util.Arrays;
import java.util.List;

import org.zeromq.ZActor;
import org.zeromq.ZActor.Actor;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZPoller;

//
//Hello World server in Java
//Binds REP socket to tcp://*:5555
//Expects "Hello" from client, replies with "World"
//
public class Hwserver
{
    public static void main(String[] args) throws Exception
    {
        Actor actor = new ZActor.SimpleActor()
        {
            @Override
            public List<Socket> createSockets(ZContext ctx, Object... args)
            {
                return Arrays.asList(ctx.createSocket(ZMQ.REP));
            }

            @Override
            public void start(Socket pipe, List<Socket> sockets, ZPoller poller)
            {
                Socket socket = sockets.get(0);
                socket.bind("tcp://*:5555");
                poller.register(socket, ZPoller.IN);
            }

            @Override
            public boolean stage(Socket socket, Socket pipe, ZPoller poller, int events)
            {
                byte[] reply = socket.recv(0);
                System.out.println(String.format("Received : [%s]", new String(reply, ZMQ.CHARSET)));

                String response = "world";
                socket.send(response.getBytes(ZMQ.CHARSET), 0);

                return true;
            }
        };
        ZActor rep = new ZActor(actor, "motdelafin");

        ZMQ.sleep(20); // let the server work ...

        rep.send("anything-sent-will-end-the-server");
        rep.exit().awaitSilent();
    }
}
