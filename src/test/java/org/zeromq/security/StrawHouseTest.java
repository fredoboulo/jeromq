package org.zeromq.security;

import org.junit.Test;

import java.io.IOException;

//  The Strawhouse Pattern
//
//  We allow or deny clients according to their IP address. It may keep
//  spammers and idiots away, but won't stop a real attacker for more
//  than a heartbeat.
public class StrawHouseTest extends AbstractSecurityTest
{
    private static class Server extends AbstractServer
    {
        @Override
        void configure()
        {
            // Set ZAP Domain to activate ZAP
            socket.setZAPDomain("global");
        }
    }

    @Test
    public void testSuccess() throws IOException
    {
        assertSuccess(Server::new, (client, server) -> {
            //  Whitelist our address; any other address will be rejected
            server.auth.allow("127.0.0.1");
        });
    }

    @Test
    public void testStrawHouseAccessDenied() throws IOException
    {
        assertAccessDenied(Server::new, (client, server) -> {
            //  Whitelist any other address than ours
            server.auth.allow("not-us");
        });
    }
}
