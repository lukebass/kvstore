package com.s62023080.CPEN431.A4;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.extensions.TestSetup;
import java.io.IOException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import ca.NetSysLab.ProtocolBuffers.KeyValueRequest.KVRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse.KVResponse;

/**
 * Unit test for simple App.
 */
public class AppTest extends TestCase {
    private Client client;
    private Server server;

    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(AppTest.class);
    }

    public void setUp() throws SocketException, UnknownHostException {
         this.server = new Server(3080);
         this.server.start();
         this.client = new Client("localhost", 3080, 100, 4);
    }

    public void tearDown() {
        this.client.close();
        this.server.close();
    }

    public void testClear()
    {
        try {
            KVRequest.Builder kvRequest = KVRequest.newBuilder();
            kvRequest.setCommand(5);
            KVResponse kvResponse = KVResponse.parseFrom(this.client.fetch(kvRequest.build().toByteArray()));
            assertEquals(0, kvResponse.getErrCode());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void testIsAlive()
    {
        try {
            KVRequest.Builder kvRequest = KVRequest.newBuilder();
            kvRequest.setCommand(6);
            KVResponse kvResponse = KVResponse.parseFrom(this.client.fetch(kvRequest.build().toByteArray()));
            assertEquals(0, kvResponse.getErrCode());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void testPID()
    {
        try {
            KVRequest.Builder kvRequest = KVRequest.newBuilder();
            kvRequest.setCommand(7);
            KVResponse kvResponse = KVResponse.parseFrom(this.client.fetch(kvRequest.build().toByteArray()));
            assertEquals(0, kvResponse.getErrCode());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void testMembershipCount()
    {
        try {
            KVRequest.Builder kvRequest = KVRequest.newBuilder();
            kvRequest.setCommand(8);
            KVResponse kvResponse = KVResponse.parseFrom(this.client.fetch(kvRequest.build().toByteArray()));
            ByteBuffer buffer = ByteBuffer.wrap(kvResponse.getValue().toByteArray());
            assertEquals(0, kvResponse.getErrCode());
            assertEquals(1, buffer.getInt());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
