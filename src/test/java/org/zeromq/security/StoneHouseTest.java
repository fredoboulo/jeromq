package org.zeromq.security;

import org.junit.Test;
import org.zeromq.ZAuth;
import org.zeromq.ZCert;

import java.io.IOException;

//  The Stonehouse Pattern
//
//  Where we allow any clients to connect, but we promise clients
//  that we are who we claim to be, and our conversations won't be
//  tampered with or modified, or spied on.
public class StoneHouseTest extends AbstractSecurityTest
{
    private static class Server extends AbstractServer
    {
        @Override
        void configure()
        {
            //  Tell the authenticator how to handle CURVE requests
            auth.configureCurve(ZAuth.CURVE_ALLOW_ANY);

            //  We need one certificate for the server.
            ZCert cert = new ZCert();
            cert.apply(socket);
            socket.setCurveServer(true);
        }
    }

    @Test
    public void testSuccess() throws IOException
    {
        assertSuccess(Server::new, (client, server) -> {
            //  We need one certificate for the client.
            ZCert clientCert = new ZCert();
            clientCert.apply(client);

            // The client must know the server's public key to make a CURVE connection.
            client.setCurveServerKey(server.socket.getCurvePublicKey());
        });
    }

    @Test
    public void testAccessDenied() throws IOException
    {
        assertAccessDenied(Server::new, (client, server) -> {
            //  We need one certificate for the client.
            ZCert clientCert = new ZCert();
            clientCert.apply(client);

            // tamper the server key
            byte[] serverPublicKey = server.socket.getCurvePublicKey();
            serverPublicKey[0] = (byte) (serverPublicKey[0] + 1);
            client.setCurveServerKey(serverPublicKey);
        });
    }
}
