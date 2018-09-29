package org.zeromq;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.zeromq.ZMQ.Curve.KeyPair;
import org.zeromq.util.ZMetadata;

public class ZCertTest
{
    private static final String CERTIFICATE = "target/cert.test.txt";

    @Before
    public void setup() throws IOException
    {
        Files.deleteIfExists(Paths.get(CERTIFICATE));
    }

    @After
    public void tearDown() throws IOException
    {
        Files.deleteIfExists(Paths.get(CERTIFICATE));
    }

    @Test
    public void testPublicKeyOnly() throws IOException
    {
        KeyPair keyPair = ZMQ.Curve.generateKeyPair();
        ZCert cert = new ZCert(keyPair.publicKey);
        assertThat(cert.getPublicKeyAsZ85(), is(keyPair.publicKey));
        assertThat(cert.getSecretKey(), nullValue());
    }

    @Test
    public void testAllKeys() throws IOException
    {
        KeyPair keyPair = ZMQ.Curve.generateKeyPair();
        ZCert cert = new ZCert(keyPair.publicKey, keyPair.secretKey);
        assertThat(cert.getPublicKeyAsZ85(), is(keyPair.publicKey));
        assertThat(cert.getSecretKeyAsZ85(), is(keyPair.secretKey));
    }

    @Test
    public void testAllKeysBinary() throws IOException
    {
        String key = new String(new byte[32], ZMQ.CHARSET);
        ZCert cert = new ZCert(key, key);
        assertThat(cert.getPublicKeyAsZ85(), is("0000000000000000000000000000000000000000"));
        assertThat(cert.getSecretKeyAsZ85(), is("0000000000000000000000000000000000000000"));
    }

    @Test
    public void testMetadata() throws IOException
    {
        ZCert cert = new ZCert();
        cert.setMeta("key", "value");
        ZMetadata metadata = cert.getMetadata();
        assertThat(metadata.get("key"), is("value"));
    }

    @Test
    public void testSaveSecret() throws IOException
    {
        ZCert cert = new ZCert();
        File secret = cert.saveSecret(CERTIFICATE);
        assertThat(secret.exists(), is(true));
    }

    @Test
    public void testSavePublic() throws IOException
    {
        ZCert cert = new ZCert();
        File file = cert.savePublic(CERTIFICATE);
        assertThat(file.exists(), is(true));
    }
}
