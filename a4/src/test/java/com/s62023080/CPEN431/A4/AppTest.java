package com.s62023080.CPEN431.A4;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import ca.NetSysLab.ProtocolBuffers.KeyValueRequest.KVRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse.KVResponse;

public class AppTest {
    private static Client client;

    private static Server server;

    @BeforeAll
    static void setup() throws IOException {
        server = new Server(3080, 3000);
        server.start();
        client = new Client("localhost", 3080, 100, 3);
    }

    @AfterEach
    void after() {
        server.clear();
    }

    @AfterAll
    static void end() {
        client.close();
        server.shutdown();
    }

    // Command 1
    @Test
    void testPutKeyFail()
    {
        try {
            KVRequest.Builder kvRequest = KVRequest.newBuilder();
            kvRequest.setCommand(1);
            KVResponse kvResponse = KVResponse.parseFrom(client.fetch(kvRequest.build().toByteArray()));
            assertEquals(6, kvResponse.getErrCode());
            assertEquals(0, server.getStore().size());
            assertEquals(1, server.getCache().size());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Command 1
    @Test
    void testPutValueFail()
    {
        try {
            KVRequest.Builder kvRequest = KVRequest.newBuilder();
            kvRequest.setCommand(1);
            kvRequest.setKey(ByteString.copyFrom(new byte[]{1}));
            KVResponse kvResponse = KVResponse.parseFrom(client.fetch(kvRequest.build().toByteArray()));
            assertEquals(7, kvResponse.getErrCode());
            assertEquals(0, server.getStore().size());
            assertEquals(1, server.getCache().size());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Command 1
    @Test
    void testPutSuccess()
    {
        try {
            KVRequest.Builder kvRequest = KVRequest.newBuilder();
            kvRequest.setCommand(1);
            kvRequest.setKey(ByteString.copyFrom(new byte[]{1}));
            kvRequest.setValue(ByteString.copyFrom(new byte[]{1,2,3}));
            kvRequest.setVersion(1);
            KVResponse kvResponse = KVResponse.parseFrom(client.fetch(kvRequest.build().toByteArray()));
            assertEquals(0, kvResponse.getErrCode());
            assertEquals(1, server.getStore().size());
            assertEquals(1, server.getCache().size());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Command 2
    @Test
    void testGetKeyFail()
    {
        try {
            KVRequest.Builder kvRequest = KVRequest.newBuilder();
            kvRequest.setCommand(2);
            KVResponse kvResponse = KVResponse.parseFrom(client.fetch(kvRequest.build().toByteArray()));
            assertEquals(6, kvResponse.getErrCode());
            assertEquals(0, server.getStore().size());
            assertEquals(1, server.getCache().size());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Command 2
    @Test
    void testGetMissingFail()
    {
        try {
            KVRequest.Builder kvRequest = KVRequest.newBuilder();
            kvRequest.setCommand(2);
            kvRequest.setKey(ByteString.copyFrom(new byte[]{2}));
            KVResponse kvResponse = KVResponse.parseFrom(client.fetch(kvRequest.build().toByteArray()));
            assertEquals(1, kvResponse.getErrCode());
            assertEquals(0, server.getStore().size());
            assertEquals(1, server.getCache().size());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Command 3
    @Test
    void testRemoveMissingFail()
    {
        try {
            KVRequest.Builder kvRequest = KVRequest.newBuilder();
            kvRequest.setCommand(3);
            kvRequest.setKey(ByteString.copyFrom(new byte[]{3}));
            KVResponse kvResponse = KVResponse.parseFrom(client.fetch(kvRequest.build().toByteArray()));
            assertEquals(1, kvResponse.getErrCode());
            assertEquals(0, server.getStore().size());
            assertEquals(1, server.getCache().size());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Command 5
    @Test
    void testClear()
    {
        try {
            KVRequest.Builder kvRequest = KVRequest.newBuilder();
            kvRequest.setCommand(1);
            kvRequest.setKey(ByteString.copyFrom(new byte[]{1}));
            kvRequest.setValue(ByteString.copyFrom(new byte[]{1,2,3}));
            kvRequest.setVersion(1);
            KVResponse kvResponse = KVResponse.parseFrom(client.fetch(kvRequest.build().toByteArray()));
            assertEquals(0, kvResponse.getErrCode());
            assertEquals(1, server.getStore().size());
            assertEquals(1, server.getCache().size());

            kvRequest = KVRequest.newBuilder();
            kvRequest.setCommand(5);
            kvResponse = KVResponse.parseFrom(client.fetch(kvRequest.build().toByteArray()));
            assertEquals(0, kvResponse.getErrCode());
            assertEquals(0, server.getStore().size());
            assertEquals(2, server.getCache().size());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Command 6
    @Test
    void testIsAlive()
    {
        try {
            KVRequest.Builder kvRequest = KVRequest.newBuilder();
            kvRequest.setCommand(6);
            KVResponse kvResponse = KVResponse.parseFrom(client.fetch(kvRequest.build().toByteArray()));
            assertEquals(0, kvResponse.getErrCode());
            assertEquals(0, server.getStore().size());
            assertEquals(1, server.getCache().size());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Command 7
    @Test
    void testPID()
    {
        try {
            KVRequest.Builder kvRequest = KVRequest.newBuilder();
            kvRequest.setCommand(7);
            KVResponse kvResponse = KVResponse.parseFrom(client.fetch(kvRequest.build().toByteArray()));
            assertEquals(0, kvResponse.getErrCode());
            assertEquals(0, server.getStore().size());
            assertEquals(1, server.getCache().size());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Command 8
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
            assertEquals(0, server.getStore().size());
            assertEquals(1, server.getCache().size());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Command Invalid
    @Test
    void testInvalid()
    {
        try {
            KVRequest.Builder kvRequest = KVRequest.newBuilder();
            kvRequest.setCommand(9);
            KVResponse kvResponse = KVResponse.parseFrom(client.fetch(kvRequest.build().toByteArray()));
            assertEquals(5, kvResponse.getErrCode());
            assertEquals(0, server.getStore().size());
            assertEquals(1, server.getCache().size());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Command 1 + 1
    @Test
    void testPutPutSuccess()
    {
        try {
            KVRequest.Builder kvRequest = KVRequest.newBuilder();
            kvRequest.setCommand(1);
            kvRequest.setKey(ByteString.copyFrom(new byte[]{1}));
            kvRequest.setValue(ByteString.copyFrom(new byte[]{1,2,3}));
            kvRequest.setVersion(1);
            KVResponse kvResponse = KVResponse.parseFrom(client.fetch(kvRequest.build().toByteArray()));
            assertEquals(0, kvResponse.getErrCode());
            assertEquals(1, server.getStore().size());
            assertEquals(1, server.getCache().size());

            kvRequest.setCommand(1);
            kvRequest.setKey(ByteString.copyFrom(new byte[]{2}));
            kvRequest.setValue(ByteString.copyFrom(new byte[]{1,2,3}));
            kvRequest.setVersion(2);
            kvResponse = KVResponse.parseFrom(client.fetch(kvRequest.build().toByteArray()));
            assertEquals(0, kvResponse.getErrCode());
            assertEquals(2, server.getStore().size());
            assertEquals(2, server.getCache().size());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Command 1 + 2
    @Test
    void testPutGetSuccess()
    {
        try {
            KVRequest.Builder kvRequest = KVRequest.newBuilder();
            kvRequest.setCommand(1);
            kvRequest.setKey(ByteString.copyFrom(new byte[]{1}));
            kvRequest.setValue(ByteString.copyFrom(new byte[]{1,2,3}));
            kvRequest.setVersion(1);
            KVResponse kvResponse = KVResponse.parseFrom(client.fetch(kvRequest.build().toByteArray()));
            assertEquals(0, kvResponse.getErrCode());
            assertEquals(1, server.getStore().size());
            assertEquals(1, server.getCache().size());

            kvRequest.setCommand(2);
            kvRequest.setKey(ByteString.copyFrom(new byte[]{1}));
            kvResponse = KVResponse.parseFrom(client.fetch(kvRequest.build().toByteArray()));
            assertEquals(0, kvResponse.getErrCode());
            assertEquals(1, kvResponse.getVersion());
            assertEquals(ByteString.copyFrom(new byte[]{1,2,3}), kvResponse.getValue());
            assertEquals(1, server.getStore().size());
            assertEquals(2, server.getCache().size());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Command 1 + 2
    @Test
    void testPutGetFail()
    {
        try {
            KVRequest.Builder kvRequest = KVRequest.newBuilder();
            kvRequest.setCommand(1);
            kvRequest.setKey(ByteString.copyFrom(new byte[]{1}));
            kvRequest.setValue(ByteString.copyFrom(new byte[]{1,2,3}));
            kvRequest.setVersion(1);
            KVResponse kvResponse = KVResponse.parseFrom(client.fetch(kvRequest.build().toByteArray()));
            assertEquals(0, kvResponse.getErrCode());
            assertEquals(1, server.getStore().size());
            assertEquals(1, server.getCache().size());

            kvRequest.setCommand(2);
            kvRequest.setKey(ByteString.copyFrom(new byte[]{5}));
            kvResponse = KVResponse.parseFrom(client.fetch(kvRequest.build().toByteArray()));
            assertEquals(1, kvResponse.getErrCode());
            assertEquals(1, server.getStore().size());
            assertEquals(2, server.getCache().size());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Command 1 + 3
    @Test
    void testPutRemoveSuccess()
    {
        try {
            KVRequest.Builder kvRequest = KVRequest.newBuilder();
            kvRequest.setCommand(1);
            kvRequest.setKey(ByteString.copyFrom(new byte[]{1}));
            kvRequest.setValue(ByteString.copyFrom(new byte[]{1,2,3}));
            kvRequest.setVersion(1);
            KVResponse kvResponse = KVResponse.parseFrom(client.fetch(kvRequest.build().toByteArray()));
            assertEquals(0, kvResponse.getErrCode());
            assertEquals(1, server.getStore().size());
            assertEquals(1, server.getCache().size());

            kvRequest.setCommand(3);
            kvRequest.setKey(ByteString.copyFrom(new byte[]{1}));
            kvResponse = KVResponse.parseFrom(client.fetch(kvRequest.build().toByteArray()));
            assertEquals(0, kvResponse.getErrCode());
            assertEquals(0, server.getStore().size());
            assertEquals(2, server.getCache().size());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Command 1 + 2 + 2
    @Test
    void testPutGetGetSuccess()
    {
        try {
            KVRequest.Builder kvRequest = KVRequest.newBuilder();
            kvRequest.setCommand(1);
            kvRequest.setKey(ByteString.copyFrom(new byte[]{1}));
            kvRequest.setValue(ByteString.copyFrom(new byte[]{1,2,3}));
            kvRequest.setVersion(1);
            KVResponse kvResponse = KVResponse.parseFrom(client.fetch(kvRequest.build().toByteArray()));
            assertEquals(0, kvResponse.getErrCode());
            assertEquals(1, server.getStore().size());
            assertEquals(1, server.getCache().size());

            kvRequest.setCommand(2);
            kvRequest.setKey(ByteString.copyFrom(new byte[]{1}));
            kvResponse = KVResponse.parseFrom(client.fetch(kvRequest.build().toByteArray()));
            assertEquals(0, kvResponse.getErrCode());
            assertEquals(1, kvResponse.getVersion());
            assertEquals(ByteString.copyFrom(new byte[]{1,2,3}), kvResponse.getValue());
            assertEquals(1, server.getStore().size());
            assertEquals(2, server.getCache().size());

            kvRequest.setCommand(2);
            kvRequest.setKey(ByteString.copyFrom(new byte[]{1}));
            kvResponse = KVResponse.parseFrom(client.fetch(kvRequest.build().toByteArray()));
            assertEquals(0, kvResponse.getErrCode());
            assertEquals(1, kvResponse.getVersion());
            assertEquals(ByteString.copyFrom(new byte[]{1,2,3}), kvResponse.getValue());
            assertEquals(1, server.getStore().size());
            assertEquals(3, server.getCache().size());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Command 1 + 3 + 2
    @Test
    void testPutRemoveGetFail()
    {
        try {
            KVRequest.Builder kvRequest = KVRequest.newBuilder();
            kvRequest.setCommand(1);
            kvRequest.setKey(ByteString.copyFrom(new byte[]{1}));
            kvRequest.setValue(ByteString.copyFrom(new byte[]{1,2,3}));
            kvRequest.setVersion(1);
            KVResponse kvResponse = KVResponse.parseFrom(client.fetch(kvRequest.build().toByteArray()));
            assertEquals(0, kvResponse.getErrCode());
            assertEquals(1, server.getStore().size());
            assertEquals(1, server.getCache().size());

            kvRequest.setCommand(3);
            kvRequest.setKey(ByteString.copyFrom(new byte[]{1}));
            kvResponse = KVResponse.parseFrom(client.fetch(kvRequest.build().toByteArray()));
            assertEquals(0, kvResponse.getErrCode());
            assertEquals(0, server.getStore().size());
            assertEquals(2, server.getCache().size());

            kvRequest.setCommand(2);
            kvRequest.setKey(ByteString.copyFrom(new byte[]{1}));
            kvResponse = KVResponse.parseFrom(client.fetch(kvRequest.build().toByteArray()));
            assertEquals(1, kvResponse.getErrCode());
            assertEquals(0, server.getStore().size());
            assertEquals(3, server.getCache().size());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Command 1 + 3 + 3
    @Test
    void testPutRemoveRemoveFail()
    {
        try {
            KVRequest.Builder kvRequest = KVRequest.newBuilder();
            kvRequest.setCommand(1);
            kvRequest.setKey(ByteString.copyFrom(new byte[]{1}));
            kvRequest.setValue(ByteString.copyFrom(new byte[]{1,2,3}));
            kvRequest.setVersion(1);
            KVResponse kvResponse = KVResponse.parseFrom(client.fetch(kvRequest.build().toByteArray()));
            assertEquals(0, kvResponse.getErrCode());
            assertEquals(1, server.getStore().size());
            assertEquals(1, server.getCache().size());

            kvRequest.setCommand(3);
            kvRequest.setKey(ByteString.copyFrom(new byte[]{1}));
            kvResponse = KVResponse.parseFrom(client.fetch(kvRequest.build().toByteArray()));
            assertEquals(0, kvResponse.getErrCode());
            assertEquals(0, server.getStore().size());
            assertEquals(2, server.getCache().size());

            kvRequest.setCommand(3);
            kvRequest.setKey(ByteString.copyFrom(new byte[]{1}));
            kvResponse = KVResponse.parseFrom(client.fetch(kvRequest.build().toByteArray()));
            assertEquals(1, kvResponse.getErrCode());
            assertEquals(0, server.getStore().size());
            assertEquals(3, server.getCache().size());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
