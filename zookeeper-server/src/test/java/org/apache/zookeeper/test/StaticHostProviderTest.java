package org.apache.zookeeper.test;

import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.client.HostProvider;
import org.apache.zookeeper.client.StaticHostProvider;
import org.apache.zookeeper.common.Time;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StaticHostProviderTest extends ZKTestCase {
    private Random r = new Random(1);

    @Test
    public void testNextGoesRound() {
        HostProvider hostProvider = getHostProvider((byte) 2);
        InetSocketAddress first = hostProvider.next(0);
        assertTrue(first != null);
        hostProvider.next(0);
        assertEquals(first, hostProvider.next(0));
    }

    @Test
    public void testNextGoesRoundAndSleeps() {
        byte size = 2;
        HostProvider hostProvider = getHostProvider(size);
        while (size > 0) {
            hostProvider.next(0);
            --size;
        }
        long start = Time.currentElapsedTime();
        hostProvider.next(1000);
        long stop = Time.currentElapsedTime();
        assertTrue(900 <= stop - start);
    }

    @Test
    public void testNextDoesNotSleepForZero() {
        byte size = 2;
        HostProvider hostProvider = getHostProvider(size);
        while (size > 0) {
            hostProvider.next(0);
            --size;
        }
        long start = Time.currentElapsedTime();
        hostProvider.next(0);
        long stop = Time.currentElapsedTime();
        assertTrue(5 > stop - start);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyServerAddressesList() {
        HostProvider hp = new StaticHostProvider(new ArrayList<>());
    }

    @Test
    public void testInvalidHostAddresses() {
                final List<InetSocketAddress> invalidAddresses = new ArrayList<>();
        InetSocketAddress unresolved = InetSocketAddress.createUnresolved("a", 1234);
        invalidAddresses.add(unresolved);
        StaticHostProvider.Resolver resolver = new StaticHostProvider.Resolver() {
            @Override
            public InetAddress[] getAllByName(String name) throws UnknownHostException {
                throw new UnknownHostException();
            }
        };
        StaticHostProvider sp = new StaticHostProvider(invalidAddresses, resolver);

                InetSocketAddress n1 = sp.next(0);
        assertTrue("Provider should return unresolved address is host is unresolvable", n1.isUnresolved());
        assertSame("Provider should return original address is host is unresolvable", unresolved, n1);
    }

    @Test
    public void testTwoConsequitiveCallsToNextReturnDifferentElement() {
        HostProvider hostProvider = getHostProvider((byte) 2);
        assertNotSame(hostProvider.next(0), hostProvider.next(0));
    }

    @Test
    public void testOnConnectDoesNotReset() {
        HostProvider hostProvider = getHostProvider((byte) 2);
        InetSocketAddress first = hostProvider.next(0);
        hostProvider.onConnected();
        InetSocketAddress second = hostProvider.next(0);
        assertNotSame(first, second);
    }

    

    private final double slackPercent = 10;
    private final int numClients = 10000;

    @Test
    public void testUpdateClientMigrateOrNot() throws UnknownHostException {
        HostProvider hostProvider = getHostProvider((byte) 4);         Collection<InetSocketAddress> newList = getServerAddresses((byte) 3); 
        InetSocketAddress myServer = new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 10, 10, 3}), 1237);

                boolean disconnectRequired = hostProvider.updateServerList(newList, myServer);
        assertFalse(disconnectRequired);
        hostProvider.onConnected();
        
                disconnectRequired = hostProvider.updateServerList(newList, myServer);
        assertFalse(disconnectRequired);
        hostProvider.onConnected();

                        newList = getServerAddresses((byte) 2);         disconnectRequired = hostProvider.updateServerList(newList, myServer);
        assertTrue(disconnectRequired);
        hostProvider.onConnected();

                        disconnectRequired = hostProvider.updateServerList(newList, myServer);
        assertTrue(disconnectRequired);
        hostProvider.onConnected();

                newList = new ArrayList<InetSocketAddress>(3);
        for (byte i = 4; i > 1; i--) {             newList.add(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 10, 10, i}), 1234 + i));
        }
        myServer = new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 10, 10, 1}), 1235);
        disconnectRequired = hostProvider.updateServerList(newList, myServer);
        assertTrue(disconnectRequired);
        hostProvider.onConnected();

                                                HostProvider[] hostProviderArray = new HostProvider[numClients];
        newList = getServerAddresses((byte) 10);
        int numDisconnects = 0;
        for (int i = 0; i < numClients; i++) {
            hostProviderArray[i] = getHostProvider((byte) 9);
            disconnectRequired = hostProviderArray[i].updateServerList(newList, myServer);
            if (disconnectRequired)
                numDisconnects++;
        }
        hostProvider.onConnected();

                assertTrue(numDisconnects < upperboundCPS(numClients, 10));
    }

    @Test
    public void testUpdateMigrationGoesRound() throws UnknownHostException {
        HostProvider hostProvider = getHostProvider((byte) 4);
                Collection<InetSocketAddress> newList = new ArrayList<InetSocketAddress>(10);
        for (byte i = 12; i > 2; i--) {                                                    newList.add(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 10, 10, i}), 1234 + i));
        }

                Collection<InetSocketAddress> oldStaying = new ArrayList<InetSocketAddress>(2);
        for (byte i = 4; i > 2; i--) {             oldStaying.add(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 10, 10, i}), 1234 + i));
        }

                Collection<InetSocketAddress> newComing = new ArrayList<InetSocketAddress>(10);
        for (byte i = 12; i > 4; i--) {            newComing.add(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 10, 10, i}), 1234 + i));
        }

                                
        boolean disconnectRequired = hostProvider.updateServerList(newList, new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 10, 10, 1}), 1235));
        assertTrue(disconnectRequired);

                                ArrayList<InetSocketAddress> seen = new ArrayList<InetSocketAddress>();
        for (int i = 0; i < newComing.size(); i++) {
            InetSocketAddress addr = hostProvider.next(0);
            assertTrue(newComing.contains(addr));
            assertTrue(!seen.contains(addr));
            seen.add(addr);
        }

                seen.clear();
        for (int i = 0; i < oldStaying.size(); i++) {
            InetSocketAddress addr = hostProvider.next(0);
            assertTrue(oldStaying.contains(addr));
            assertTrue(!seen.contains(addr));
            seen.add(addr);
        }

                        InetSocketAddress first = hostProvider.next(0);
        assertTrue(first != null);
        for (int i = 0; i < newList.size() - 1; i++) {
            hostProvider.next(0);
        }

        assertEquals(first, hostProvider.next(0));
        hostProvider.onConnected();
    }

    @Test
    public void testUpdateLoadBalancing() throws UnknownHostException {
                boolean disconnectRequired;
        HostProvider[] hostProviderArray = new HostProvider[numClients];
        InetSocketAddress[] curHostForEachClient = new InetSocketAddress[numClients];
        int[] numClientsPerHost = new int[9];

                for (int i = 0; i < numClients; i++) {
            hostProviderArray[i] = getHostProvider((byte) 9);
            curHostForEachClient[i] = hostProviderArray[i].next(0);
            numClientsPerHost[curHostForEachClient[i].getPort() - 1235]++;
            hostProviderArray[i].onConnected();
        }

        for (int i = 0; i < 9; i++) {
            assertTrue(numClientsPerHost[i] <= upperboundCPS(numClients, 9));
            assertTrue(numClientsPerHost[i] >= lowerboundCPS(numClients, 9));
            numClientsPerHost[i] = 0;         }

                Collection<InetSocketAddress> newList = getServerAddresses((byte) 8);

        for (int i = 0; i < numClients; i++) {
            disconnectRequired = hostProviderArray[i].updateServerList(newList, curHostForEachClient[i]);
            if (disconnectRequired) curHostForEachClient[i] = hostProviderArray[i].next(0);
            numClientsPerHost[curHostForEachClient[i].getPort() - 1235]++;
            hostProviderArray[i].onConnected();
        }

        for (int i = 0; i < 8; i++) {
            assertTrue(numClientsPerHost[i] <= upperboundCPS(numClients, 8));
            assertTrue(numClientsPerHost[i] >= lowerboundCPS(numClients, 8));
            numClientsPerHost[i] = 0;         }
        assertTrue(numClientsPerHost[8] == 0);

                newList = getServerAddresses((byte) 6);

        for (int i = 0; i < numClients; i++) {
            disconnectRequired = hostProviderArray[i].updateServerList(newList, curHostForEachClient[i]);
            if (disconnectRequired) curHostForEachClient[i] = hostProviderArray[i].next(0);
            numClientsPerHost[curHostForEachClient[i].getPort() - 1235]++;
            hostProviderArray[i].onConnected();
        }

        for (int i = 0; i < 6; i++) {
            assertTrue(numClientsPerHost[i] <= upperboundCPS(numClients, 6));
            assertTrue(numClientsPerHost[i] >= lowerboundCPS(numClients, 6));
            numClientsPerHost[i] = 0;         }
        assertTrue(numClientsPerHost[6] == 0);
        assertTrue(numClientsPerHost[7] == 0);
        assertTrue(numClientsPerHost[8] == 0);

                        newList = new ArrayList<InetSocketAddress>(8);
        for (byte i = 9; i > 1; i--) {
            newList.add(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 10, 10, i}), 1234 + i));
        }

        for (int i = 0; i < numClients; i++) {
            disconnectRequired = hostProviderArray[i].updateServerList(newList, curHostForEachClient[i]);
            if (disconnectRequired) curHostForEachClient[i] = hostProviderArray[i].next(0);
            numClientsPerHost[curHostForEachClient[i].getPort() - 1235]++;
            hostProviderArray[i].onConnected();
        }

        assertTrue(numClientsPerHost[0] == 0);

        for (int i = 1; i < 9; i++) {
            assertTrue(numClientsPerHost[i] <= upperboundCPS(numClients, 8));
            assertTrue(numClientsPerHost[i] >= lowerboundCPS(numClients, 8));
            numClientsPerHost[i] = 0;         }

                newList = getServerAddresses((byte) 9);

        for (int i = 0; i < numClients; i++) {
            disconnectRequired = hostProviderArray[i].updateServerList(newList, curHostForEachClient[i]);
            if (disconnectRequired) curHostForEachClient[i] = hostProviderArray[i].next(0);
            numClientsPerHost[curHostForEachClient[i].getPort() - 1235]++;
            hostProviderArray[i].onConnected();
        }

        for (int i = 0; i < 9; i++) {
            assertTrue(numClientsPerHost[i] <= upperboundCPS(numClients, 9));
            assertTrue(numClientsPerHost[i] >= lowerboundCPS(numClients, 9));
        }
    }

    @Test
    public void testNoCurrentHostDuringNormalMode() throws UnknownHostException {
                boolean disconnectRequired;
        StaticHostProvider[] hostProviderArray = new StaticHostProvider[numClients];
        InetSocketAddress[] curHostForEachClient = new InetSocketAddress[numClients];
        int[] numClientsPerHost = new int[9];

                for (int i = 0; i < numClients; i++) {
            hostProviderArray[i] = getHostProvider((byte) 9);
            if (i >= (numClients / 2)) {
                curHostForEachClient[i] = hostProviderArray[i].next(0);
            } else {
                                                curHostForEachClient[i] = null;
            }
        }

                Collection<InetSocketAddress> newList = getServerAddresses((byte) 7);

        for (int i = 0; i < numClients; i++) {
                                    disconnectRequired = hostProviderArray[i].updateServerList(newList,
                    curHostForEachClient[i]);
            if (disconnectRequired)
                curHostForEachClient[i] = hostProviderArray[i].next(0);
            else if (curHostForEachClient[i] == null) {
                                curHostForEachClient[i] = hostProviderArray[i]
                        .getServerAtIndex(0);
            }
            numClientsPerHost[curHostForEachClient[i].getPort() - 1235]++;
                        hostProviderArray[i].onConnected();
        }

        for (int i = 0; i < 7; i++) {
            assertTrue(numClientsPerHost[i] <= upperboundCPS(numClients, 7));
            assertTrue(numClientsPerHost[i] >= lowerboundCPS(numClients, 7));
            numClientsPerHost[i] = 0;         }
        assertTrue(numClientsPerHost[7] == 0);
        assertTrue(numClientsPerHost[8] == 0);

                newList = getServerAddresses((byte) 8);

        for (int i = 0; i < numClients; i++) {
            InetSocketAddress myServer = (i < (numClients / 2)) ? null
                    : curHostForEachClient[i];
                        disconnectRequired = hostProviderArray[i].updateServerList(newList,
                    myServer);
            if (disconnectRequired)
                curHostForEachClient[i] = hostProviderArray[i].next(0);
            numClientsPerHost[curHostForEachClient[i].getPort() - 1235]++;
            hostProviderArray[i].onConnected();
        }

        for (int i = 0; i < 8; i++) {
            assertTrue(numClientsPerHost[i] <= upperboundCPS(numClients, 8));
            assertTrue(numClientsPerHost[i] >= lowerboundCPS(numClients, 8));
        }
    }

    @Test
    public void testReconfigDuringReconfigMode() throws UnknownHostException {
                boolean disconnectRequired;
        StaticHostProvider[] hostProviderArray = new StaticHostProvider[numClients];
        InetSocketAddress[] curHostForEachClient = new InetSocketAddress[numClients];
        int[] numClientsPerHost = new int[9];

                for (int i = 0; i < numClients; i++) {
            hostProviderArray[i] = getHostProvider((byte) 9);
            curHostForEachClient[i] = hostProviderArray[i].next(0);
        }

                Collection<InetSocketAddress> newList = getServerAddresses((byte) 7);

        for (int i = 0; i < numClients; i++) {
                        hostProviderArray[i].updateServerList(newList,
                    curHostForEachClient[i]);
        }

                        newList = getServerAddresses((byte) 9);

        for (int i = 0; i < numClients; i++) {
            InetSocketAddress myServer = (i < (numClients / 2)) ? null
                    : curHostForEachClient[i];
                                                            disconnectRequired = hostProviderArray[i].updateServerList(newList,
                    myServer);
            if (disconnectRequired)
                curHostForEachClient[i] = hostProviderArray[i].next(0);
            else {
                                                curHostForEachClient[i] = hostProviderArray[i]
                        .getServerAtCurrentIndex();
            }
            numClientsPerHost[curHostForEachClient[i].getPort() - 1235]++;
            hostProviderArray[i].onConnected();
        }

        for (int i = 0; i < 9; i++) {
            assertTrue(numClientsPerHost[i] <= upperboundCPS(numClients, 9));
            assertTrue(numClientsPerHost[i] >= lowerboundCPS(numClients, 9));
        }
    }

    private StaticHostProvider getHostProvider(byte size) {
        return new StaticHostProvider(getServerAddresses(size), r.nextLong());
    }

    private Collection<InetSocketAddress> getServerAddresses(byte size)   {
        ArrayList<InetSocketAddress> list = new ArrayList<InetSocketAddress>(
                size);
        while (size > 0) {
            try {
                list.add(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 10, 10, size}), 1234 + size));
            } catch (UnknownHostException e) {
                                e.printStackTrace();
            }
            --size;
        }
        return list;
    }

    

    
    @Test
    public void testUpdateServerList_UnresolvedHostnames_NoDisconnection1() {
                        HostProvider hostProvider = getHostProviderWithUnresolvedHostnames(4);
                Collection<InetSocketAddress> newList = getUnresolvedHostnames(3);
        InetSocketAddress myServer = InetSocketAddress.createUnresolved("testhost-3.testdomain.com", 1237);

                boolean disconnectRequired = hostProvider.updateServerList(newList, myServer);

                assertFalse(disconnectRequired);
        hostProvider.onConnected();
    }

    
    @Test
    public void testUpdateServerList_UnresolvedHostnames_NoDisconnection2() {
                        HostProvider hostProvider = getHostProviderWithUnresolvedHostnames(3);
                Collection<InetSocketAddress> newList = getUnresolvedHostnames(3);
        InetSocketAddress myServer = InetSocketAddress.createUnresolved("testhost-3.testdomain.com", 1237);

                boolean disconnectRequired = hostProvider.updateServerList(newList, myServer);

                assertFalse(disconnectRequired);
        hostProvider.onConnected();
    }

    
    @Test
    public void testUpdateServerList_UnresolvedHostnames_Disconnection1() {
                        HostProvider hostProvider = getHostProviderWithUnresolvedHostnames(3);
                Collection<InetSocketAddress> newList = getUnresolvedHostnames(2);
        InetSocketAddress myServer = InetSocketAddress.createUnresolved("testhost-3.testdomain.com", 1237);

                boolean disconnectRequired = hostProvider.updateServerList(newList, myServer);

                assertTrue(disconnectRequired);
        hostProvider.onConnected();
    }

    
    @Test
    public void testUpdateServerList_UnresolvedHostnames_Disconnection2() {
                        HostProvider hostProvider = getHostProviderWithUnresolvedHostnames(3);
                Collection<InetSocketAddress> newList = getUnresolvedHostnames(3);
        InetSocketAddress myServer = InetSocketAddress.createUnresolved("testhost-4.testdomain.com", 1237);

                boolean disconnectRequired = hostProvider.updateServerList(newList, myServer);

                assertTrue(disconnectRequired);
        hostProvider.onConnected();
    }

    @Test
    public void testUpdateServerList_ResolvedWithUnResolvedAddress_ForceDisconnect() {
                        List<InetSocketAddress> addresses = Collections.singletonList(
                InetSocketAddress.createUnresolved("testhost-1.resolvable.zk", 1235)
        );
        HostProvider hostProvider = new StaticHostProvider(addresses, new TestResolver());
        InetSocketAddress currentHost = hostProvider.next(100);
        assertThat("CurrentHost is which the client is currently connecting to, it should be resolved",
                currentHost.isUnresolved(), is(false));

                InetSocketAddress replaceHost = InetSocketAddress.createUnresolved("testhost-1.resolvable.zk", 1235);
        assertThat("Replace host must be unresolved in this test case", replaceHost.isUnresolved(), is(true));
        boolean disconnect = hostProvider.updateServerList(new ArrayList<>(
            Collections.singletonList(replaceHost)),
            currentHost
        );

                assertThat(disconnect, is(false));
    }

    @Test
    public void testUpdateServerList_ResolvedWithResolvedAddress_NoDisconnect() throws UnknownHostException {
                        List<InetSocketAddress> addresses = Collections.singletonList(
                InetSocketAddress.createUnresolved("testhost-1.resolvable.zk", 1235)
        );
        HostProvider hostProvider = new StaticHostProvider(addresses, new TestResolver());
        InetSocketAddress currentHost = hostProvider.next(100);
        assertThat("CurrentHost is which the client is currently connecting to, it should be resolved",
                currentHost.isUnresolved(), is(false));

                InetSocketAddress replaceHost =
                new InetSocketAddress(InetAddress.getByAddress(currentHost.getHostString(),
                        currentHost.getAddress().getAddress()), currentHost.getPort());
        assertThat("Replace host must be resolved in this test case", replaceHost.isUnresolved(), is(false));
        boolean disconnect = hostProvider.updateServerList(new ArrayList<>(
            Collections.singletonList(replaceHost)),
            currentHost
        );

                assertThat(disconnect, equalTo(false));
    }

    @Test
    public void testUpdateServerList_UnResolvedWithUnResolvedAddress_ForceDisconnect() {
                        List<InetSocketAddress> addresses = Collections.singletonList(
                InetSocketAddress.createUnresolved("testhost-1.zookeepertest.zk", 1235)
        );
        HostProvider hostProvider = new StaticHostProvider(addresses, new TestResolver());
        InetSocketAddress currentHost = hostProvider.next(100);
        assertThat("CurrentHost is not resolvable in this test case",
                currentHost.isUnresolved(), is(true));

                InetSocketAddress replaceHost = InetSocketAddress.createUnresolved("testhost-1.resolvable.zk", 1235);
        assertThat("Replace host must be unresolved in this test case", replaceHost.isUnresolved(), is(true));
        boolean disconnect = hostProvider.updateServerList(new ArrayList<>(
                        Collections.singletonList(replaceHost)),
                currentHost
        );

                assertThat(disconnect, is(true));
    }

    @Test
    public void testUpdateServerList_UnResolvedWithResolvedAddress_ForceDisconnect() throws UnknownHostException {
                        List<InetSocketAddress> addresses = Collections.singletonList(
                InetSocketAddress.createUnresolved("testhost-1.zookeepertest.zk", 1235)
        );
        HostProvider hostProvider = new StaticHostProvider(addresses, new TestResolver());
        InetSocketAddress currentHost = hostProvider.next(100);
        assertThat("CurrentHost not resolvable in this test case",
                currentHost.isUnresolved(), is(true));

                byte[] addr = new byte[] { 10, 0, 0, 1 };
        InetSocketAddress replaceHost =
                new InetSocketAddress(InetAddress.getByAddress(currentHost.getHostString(), addr),
                        currentHost.getPort());
        assertThat("Replace host must be resolved in this test case", replaceHost.isUnresolved(), is(false));
        boolean disconnect = hostProvider.updateServerList(new ArrayList<>(
                        Collections.singletonList(replaceHost)),
                currentHost
        );

                assertThat(disconnect, equalTo(false));
    }

    private class TestResolver implements StaticHostProvider.Resolver {
        private byte counter = 1;

        @Override
        public InetAddress[] getAllByName(String name) throws UnknownHostException {
            if (name.contains("resolvable")) {
                byte[] addr = new byte[] { 10, 0, 0, (byte)(counter++ % 10) };
                return new InetAddress[] { InetAddress.getByAddress(name, addr) };
            }
            throw new UnknownHostException();
        }
    }

    private double lowerboundCPS(int numClients, int numServers) {
        return (1 - slackPercent/100.0) * numClients / numServers;
    }

    private double upperboundCPS(int numClients, int numServers) {
        return (1 + slackPercent/100.0) * numClients / numServers;
    }

    

    @Test
    public void testLiteralIPNoReverseNS() {
        byte size = 30;
        HostProvider hostProvider = getHostProviderUnresolved(size);
        for (int i = 0; i < size; i++) {
            InetSocketAddress next = hostProvider.next(0);
            assertThat(next, instanceOf(InetSocketAddress.class));
            assertFalse(next.isUnresolved());
            assertTrue(next.toString().startsWith("/"));
                        String hostname = next.getHostString();
                        assertEquals(next.getAddress().getHostAddress(), hostname);
        }
    }

    @Test
    public void testReResolvingSingle() throws UnknownHostException {
                byte size = 1;
        ArrayList<InetSocketAddress> list = new ArrayList<InetSocketAddress>(size);

                list.add(InetSocketAddress.createUnresolved("issues.apache.org", 1234));

        final InetAddress issuesApacheOrg = mock(InetAddress.class);
        when(issuesApacheOrg.getHostAddress()).thenReturn("192.168.1.1");
        when(issuesApacheOrg.toString()).thenReturn("issues.apache.org");
        when(issuesApacheOrg.getHostName()).thenReturn("issues.apache.org");

        StaticHostProvider.Resolver resolver = new StaticHostProvider.Resolver() {
            @Override
            public InetAddress[] getAllByName(String name) {
                return new InetAddress[] {
                        issuesApacheOrg
                };
            }
        };
        StaticHostProvider.Resolver spyResolver = spy(resolver);

                StaticHostProvider hostProvider = new StaticHostProvider(list, spyResolver);
        for (int i = 0; i < 10; i++) {
            InetSocketAddress next = hostProvider.next(0);
            assertEquals(issuesApacheOrg, next.getAddress());
        }

                        verify(spyResolver, times(10)).getAllByName("issues.apache.org");     }

    @Test
    public void testReResolvingMultiple() throws UnknownHostException {
                byte size = 1;
        ArrayList<InetSocketAddress> list = new ArrayList<InetSocketAddress>(size);

                list.add(InetSocketAddress.createUnresolved("www.apache.org", 1234));

        final InetAddress apacheOrg1 = mock(InetAddress.class);
        when(apacheOrg1.getHostAddress()).thenReturn("192.168.1.1");
        when(apacheOrg1.toString()).thenReturn("www.apache.org");
        when(apacheOrg1.getHostName()).thenReturn("www.apache.org");

        final InetAddress apacheOrg2 = mock(InetAddress.class);
        when(apacheOrg2.getHostAddress()).thenReturn("192.168.1.2");
        when(apacheOrg2.toString()).thenReturn("www.apache.org");
        when(apacheOrg2.getHostName()).thenReturn("www.apache.org");

        final List<InetAddress> resolvedAddresses = new ArrayList<InetAddress>();
        resolvedAddresses.add(apacheOrg1);
        resolvedAddresses.add(apacheOrg2);
        StaticHostProvider.Resolver resolver = new StaticHostProvider.Resolver() {
            @Override
            public InetAddress[] getAllByName(String name) {
                return resolvedAddresses.toArray(new InetAddress[resolvedAddresses.size()]);
            }
        };
        StaticHostProvider.Resolver spyResolver = spy(resolver);

                StaticHostProvider hostProvider = new StaticHostProvider(list, spyResolver);
        assertEquals(1, hostProvider.size()); 
        for (int i = 0; i < 10; i++) {
            InetSocketAddress next = hostProvider.next(0);
            assertThat("Bad IP address returned", next.getAddress().getHostAddress(), anyOf(equalTo(apacheOrg1.getHostAddress()), equalTo(apacheOrg2.getHostAddress())));
            assertEquals(1, hostProvider.size());         }
                verify(spyResolver, times(10)).getAllByName("www.apache.org");     }

    @Test
    public void testReResolveMultipleOneFailing() throws UnknownHostException {
                final List<InetSocketAddress> list = new ArrayList<InetSocketAddress>();
        list.add(InetSocketAddress.createUnresolved("www.apache.org", 1234));
        final List<String> ipList = new ArrayList<String>();
        final List<InetAddress> resolvedAddresses = new ArrayList<InetAddress>();
        for (int i = 0; i < 3; i++) {
            ipList.add(String.format("192.168.1.%d", i+1));
            final InetAddress apacheOrg = mock(InetAddress.class);
            when(apacheOrg.getHostAddress()).thenReturn(String.format("192.168.1.%d", i+1));
            when(apacheOrg.toString()).thenReturn(String.format("192.168.1.%d", i+1));
            when(apacheOrg.getHostName()).thenReturn("www.apache.org");
            resolvedAddresses.add(apacheOrg);
        }

        StaticHostProvider.Resolver resolver = new StaticHostProvider.Resolver() {
            @Override
            public InetAddress[] getAllByName(String name) {
                return resolvedAddresses.toArray(new InetAddress[resolvedAddresses.size()]);
            }
        };
        StaticHostProvider.Resolver spyResolver = spy(resolver);
        StaticHostProvider hostProvider = new StaticHostProvider(list, spyResolver);

                InetSocketAddress resolvedFirst = hostProvider.next(0);
        assertFalse("HostProvider should return resolved addresses", resolvedFirst.isUnresolved());
        assertThat("Bad IP address returned", ipList, hasItems(resolvedFirst.getAddress().getHostAddress()));

        hostProvider.onConnected(); 
        InetSocketAddress resolvedSecond = hostProvider.next(0);
        assertFalse("HostProvider should return resolved addresses", resolvedSecond.isUnresolved());
        assertThat("Bad IP address returned", ipList, hasItems(resolvedSecond.getAddress().getHostAddress()));

                
        InetSocketAddress resolvedThird = hostProvider.next(0);
        assertFalse("HostProvider should return resolved addresses", resolvedThird.isUnresolved());
        assertThat("Bad IP address returned", ipList, hasItems(resolvedThird.getAddress().getHostAddress()));

        verify(spyResolver, times(3)).getAllByName("www.apache.org");      }

    @Test
    public void testEmptyResolution() throws UnknownHostException {
                final List<InetSocketAddress> list = new ArrayList<InetSocketAddress>();
        list.add(InetSocketAddress.createUnresolved("www.apache.org", 1234));
        list.add(InetSocketAddress.createUnresolved("www.google.com", 1234));
        final List<InetAddress> resolvedAddresses = new ArrayList<InetAddress>();

        final InetAddress apacheOrg1 = mock(InetAddress.class);
        when(apacheOrg1.getHostAddress()).thenReturn("192.168.1.1");
        when(apacheOrg1.toString()).thenReturn("www.apache.org");
        when(apacheOrg1.getHostName()).thenReturn("www.apache.org");

        resolvedAddresses.add(apacheOrg1);

        StaticHostProvider.Resolver resolver = new StaticHostProvider.Resolver() {
            @Override
            public InetAddress[] getAllByName(String name) {
                if ("www.apache.org".equalsIgnoreCase(name)) {
                    return resolvedAddresses.toArray(new InetAddress[resolvedAddresses.size()]);
                } else {
                    return new InetAddress[0];
                }
            }
        };
        StaticHostProvider.Resolver spyResolver = spy(resolver);
        StaticHostProvider hostProvider = new StaticHostProvider(list, spyResolver);

                for (int i = 0; i < 10; i++) {
            InetSocketAddress resolved = hostProvider.next(0);
            hostProvider.onConnected();
            if (resolved.getHostName().equals("www.google.com")) {
                assertTrue("HostProvider should return unresolved address if host is unresolvable", resolved.isUnresolved());
            } else {
                assertFalse("HostProvider should return resolved addresses", resolved.isUnresolved());
                assertEquals("192.168.1.1", resolved.getAddress().getHostAddress());
            }
        }

        verify(spyResolver, times(5)).getAllByName("www.apache.org");
        verify(spyResolver, times(5)).getAllByName("www.google.com");
    }

    @Test
    public void testReResolvingLocalhost() {
        byte size = 2;
        ArrayList<InetSocketAddress> list = new ArrayList<InetSocketAddress>(size);

                list.add(InetSocketAddress.createUnresolved("localhost", 1234));
        list.add(InetSocketAddress.createUnresolved("localhost", 1235));
        StaticHostProvider hostProvider = new StaticHostProvider(list);
        int sizeBefore = hostProvider.size();
        InetSocketAddress next = hostProvider.next(0);
        next = hostProvider.next(0);
        assertTrue("Different number of addresses in the list: " + hostProvider.size() +
                " (after), " + sizeBefore + " (before)", hostProvider.size() == sizeBefore);
    }

    private StaticHostProvider getHostProviderUnresolved(byte size) {
        return new StaticHostProvider(getUnresolvedServerAddresses(size), r.nextLong());
    }

    private Collection<InetSocketAddress> getUnresolvedServerAddresses(byte size) {
        ArrayList<InetSocketAddress> list = new ArrayList<InetSocketAddress>(size);
        while (size > 0) {
            list.add(InetSocketAddress.createUnresolved("10.10.10." + size, 1234 + size));
            --size;
        }
        return list;
    }

    private StaticHostProvider getHostProviderWithUnresolvedHostnames(int size) {
        return new StaticHostProvider(getUnresolvedHostnames(size), r.nextLong());
    }

    private Collection<InetSocketAddress> getUnresolvedHostnames(int size) {
        ArrayList<InetSocketAddress> list = new ArrayList<>(size);
        while (size > 0) {
            list.add(InetSocketAddress.createUnresolved(String.format("testhost-%d.testdomain.com", size), 1234 + size));
            --size;
        }
        System.out.println(Arrays.toString(list.toArray()));
        return list;
    }
}
