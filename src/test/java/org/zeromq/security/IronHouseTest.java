package org.zeromq.security;

import org.junit.Test;
import org.zeromq.ZCert;

import java.io.File;
import java.io.IOException;

//  The Ironhouse Pattern
//
//  Security doesn't get any stronger than this. An attacker is going to
//  have to break into your systems to see data before/after encryption.
public class IronHouseTest extends AbstractSecurityTest
{
    private static class Server extends AbstractServer
    {
        @Override
        void configure()
        {
            //  Tell the authenticator how to handle CURVE requests
            new File("target/security/curve").mkdirs();
            auth.configureCurve("target/security/curve");

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

            // The server knows the client certificate
            clientCert.savePublic("target/security/curve/client.pub");

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

            // The client certificate is not known from the server

            // The client must know the server's public key to make a CURVE connection.
            client.setCurveServerKey(server.socket.getCurvePublicKey());
        });
    }
}
