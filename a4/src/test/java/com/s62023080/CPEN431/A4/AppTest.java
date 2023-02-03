package com.s62023080.CPEN431.A4;

import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import ca.NetSysLab.ProtocolBuffers.KeyValueRequest.KVRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse.KVResponse;

public class AppTest {
    private static Client client;

    @BeforeAll
    static void setup() throws SocketException, UnknownHostException {
        new Server(3080).start();
        client = new Client("localhost", 3080, 100, 4);
    }

    @AfterAll
    static void tearDown() {
        client.close();
    }

    @Test
    void testClear()
    {
        try {
            KVRequest.Builder kvRequest = KVRequest.newBuilder();
            kvRequest.setCommand(5);
            KVResponse kvResponse = KVResponse.parseFrom(client.fetch(kvRequest.build().toByteArray()));
            assertEquals(0, kvResponse.getErrCode());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void testIsAlive()
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

    @Test
    void testPID()
    {
        try {
            KVRequest.Builder kvRequest = KVRequest.newBuilder();
            kvRequest.setCommand(7);
            KVResponse kvResponse = KVResponse.parseFrom(client.fetch(kvRequest.build().toByteArray()));
            assertEquals(0, kvResponse.getErrCode());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void testMembershipCount()
    {
        try {
            KVRequest.Builder kvRequest = KVRequest.newBuilder();
            kvRequest.setCommand(8);
            KVResponse kvResponse = KVResponse.parseFrom(client.fetch(kvRequest.build().toByteArray()));
            ByteBuffer buffer = ByteBuffer.wrap(kvResponse.getValue().toByteArray());
            assertEquals(0, kvResponse.getErrCode());
            assertEquals(1, buffer.getInt());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
