package org.zeromq.security;

import org.junit.Test;

import java.io.IOException;

//  The Woodhouse Pattern
//
//  It may keep some malicious people out but all it takes is a bit
//  of network sniffing, and they'll be able to fake their way in.
public class WoodHouseTest extends AbstractSecurityTest
{
    private static class Server extends AbstractServer
    {
        @Override
        void configure()
        {
            //  Tell the authenticator how to handle PLAIN requests
            auth.configurePlain("unused", "src/test/resources/security/passwords");

            socket.setPlainServer(true);
        }
    }

    @Test
    public void testSuccess() throws IOException
    {
        assertSuccess(Server::new, (client, server) -> {
            client.setPlainUsername("admin");
            client.setPlainPassword("secret");
        });
    }

    @Test
    public void testAccessDeniedPassword() throws IOException
    {
        assertAccessDenied(Server::new, (client, server) -> {
            client.setPlainUsername("admin");
            client.setPlainPassword("wrong");
        });
    }

    @Test
    public void testAccessDeniedUser() throws IOException
    {
        assertAccessDenied(Server::new, (client, server) -> {
            client.setPlainUsername("unknown");
            client.setPlainPassword("secret");
        });
    }
}
