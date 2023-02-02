package com.s62023080.CPEN431.A4;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import java.net.SocketException;
import java.net.UnknownHostException;

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
    public AppTest( String testName ) {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite( AppTest.class );
    }

    @Override
    public void setUp () throws SocketException, UnknownHostException {
        this.server = new Server(3080);
        this.client = new Client("localhost", 3080, 100, 4);
    }

    public void testPut()
    {
        assertTrue( true );
    }

    public void testGet()
    {
        assertTrue( true );
    }

    public void testRemove()
    {
        assertTrue( true );
    }

    public void testShutdown()
    {
        assertTrue( true );
    }

    public void testClear()
    {
        assertTrue( true );
    }

    public void testIsAlive()
    {
        assertTrue( true );
    }

    public void testGetPID()
    {
        assertTrue( true );
    }

    public void testGetMembershipCount()
    {
        assertTrue( true );
    }
}
