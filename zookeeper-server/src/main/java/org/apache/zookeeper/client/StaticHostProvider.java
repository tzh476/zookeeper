package org.apache.zookeeper.client;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@InterfaceAudience.Public
public final class StaticHostProvider implements HostProvider {
    public interface Resolver {
        InetAddress[] getAllByName(String name) throws UnknownHostException;
    }

    private static final Logger LOG = LoggerFactory
            .getLogger(StaticHostProvider.class);

    private List<InetSocketAddress> serverAddresses = new ArrayList<InetSocketAddress>(
            5);

    private Random sourceOfRandomness;
    private int lastIndex = -1;

    private int currentIndex = -1;

    
    private boolean reconfigMode = false;

    private final List<InetSocketAddress> oldServers = new ArrayList<InetSocketAddress>(
            5);

    private final List<InetSocketAddress> newServers = new ArrayList<InetSocketAddress>(
            5);

    private int currentIndexOld = -1;
    private int currentIndexNew = -1;

    private float pOld, pNew;

    private Resolver resolver;

    
    public StaticHostProvider(Collection<InetSocketAddress> serverAddresses) {
        init(serverAddresses,
                System.currentTimeMillis() ^ this.hashCode(),
                new Resolver() {
            @Override
            public InetAddress[] getAllByName(String name) throws UnknownHostException {
                return InetAddress.getAllByName(name);
            }
        });
    }

    
    public StaticHostProvider(Collection<InetSocketAddress> serverAddresses, Resolver resolver) {
        init(serverAddresses, System.currentTimeMillis() ^ this.hashCode(), resolver);
    }

    
    public StaticHostProvider(Collection<InetSocketAddress> serverAddresses,
        long randomnessSeed) {
        init(serverAddresses, randomnessSeed, new Resolver() {
            @Override
            public InetAddress[] getAllByName(String name) throws UnknownHostException {
                return InetAddress.getAllByName(name);
            }
        });
    }

    private void init(Collection<InetSocketAddress> serverAddresses, long randomnessSeed, Resolver resolver) {
        this.sourceOfRandomness = new Random(randomnessSeed);
        this.resolver = resolver;
        if (serverAddresses.isEmpty()) {
            throw new IllegalArgumentException(
                    "A HostProvider may not be empty!");
        }
        this.serverAddresses = shuffle(serverAddresses);
        currentIndex = -1;
        lastIndex = -1;
    }

    private InetSocketAddress resolve(InetSocketAddress address) {
        try {
            String curHostString = address.getHostString();
            List<InetAddress> resolvedAddresses = new ArrayList<>(Arrays.asList(this.resolver.getAllByName(curHostString)));
            if (resolvedAddresses.isEmpty()) {
                return address;
            }
            Collections.shuffle(resolvedAddresses);
            return new InetSocketAddress(resolvedAddresses.get(0), address.getPort());
        } catch (UnknownHostException e) {
            LOG.error("Unable to resolve address: {}", address.toString(), e);
            return address;
        }
    }

    private List<InetSocketAddress> shuffle(Collection<InetSocketAddress> serverAddresses) {
        List<InetSocketAddress> tmpList = new ArrayList<>(serverAddresses.size());
        tmpList.addAll(serverAddresses);
        Collections.shuffle(tmpList, sourceOfRandomness);
        return tmpList;
    }

    
    @Override
    public synchronized boolean updateServerList(
            Collection<InetSocketAddress> serverAddresses,
            InetSocketAddress currentHost) {
        List<InetSocketAddress> shuffledList = shuffle(serverAddresses);
        if (shuffledList.isEmpty()) {
            throw new IllegalArgumentException(
                    "A HostProvider may not be empty!");
        }
                boolean myServerInNewConfig = false;

        InetSocketAddress myServer = currentHost;

                if (reconfigMode) {
            myServer = next(0);
        }

                if (myServer == null) {
                        if (lastIndex >= 0) {
                                myServer = this.serverAddresses.get(lastIndex);
            } else {
                                myServer = this.serverAddresses.get(0);
            }
        }

        for (InetSocketAddress addr : shuffledList) {
            if (addr.getPort() == myServer.getPort()
                    && ((addr.getAddress() != null
                            && myServer.getAddress() != null && addr
                            .getAddress().equals(myServer.getAddress())) || addr
                            .getHostString().equals(myServer.getHostString()))) {
                myServerInNewConfig = true;
                break;
            }
        }

        reconfigMode = true;

        newServers.clear();
        oldServers.clear();
                        for (InetSocketAddress address : shuffledList) {
            if (this.serverAddresses.contains(address)) {
                oldServers.add(address);
            } else {
                newServers.add(address);
            }
        }

        int numOld = oldServers.size();
        int numNew = newServers.size();

                if (numOld + numNew > this.serverAddresses.size()) {
            if (myServerInNewConfig) {
                                                                if (sourceOfRandomness.nextFloat() <= (1 - ((float) this.serverAddresses
                        .size()) / (numOld + numNew))) {
                    pNew = 1;
                    pOld = 0;
                } else {
                                        reconfigMode = false;
                }
            } else {
                                                                pNew = 1;
                pOld = 0;
            }
        } else {             if (myServerInNewConfig) {
                                                reconfigMode = false;
            } else {
                pOld = ((float) (numOld * (this.serverAddresses.size() - (numOld + numNew))))
                        / ((numOld + numNew) * (this.serverAddresses.size() - numOld));
                pNew = 1 - pOld;
            }
        }

        if (!reconfigMode) {
            currentIndex = shuffledList.indexOf(getServerAtCurrentIndex());
        } else {
            currentIndex = -1;
        }
        this.serverAddresses = shuffledList;
        currentIndexOld = -1;
        currentIndexNew = -1;
        lastIndex = currentIndex;
        return reconfigMode;
    }

    public synchronized InetSocketAddress getServerAtIndex(int i) {
    	if (i < 0 || i >= serverAddresses.size()) return null;
    	return serverAddresses.get(i);
    }
    
    public synchronized InetSocketAddress getServerAtCurrentIndex() {
    	return getServerAtIndex(currentIndex);
    }

    public synchronized int size() {
        return serverAddresses.size();
    }

    
    private InetSocketAddress nextHostInReconfigMode() {
        boolean takeNew = (sourceOfRandomness.nextFloat() <= pNew);

                                                if (((currentIndexNew + 1) < newServers.size())
                && (takeNew || (currentIndexOld + 1) >= oldServers.size())) {
            ++currentIndexNew;
            return newServers.get(currentIndexNew);
        }

                if ((currentIndexOld + 1) < oldServers.size()) {
            ++currentIndexOld;
            return oldServers.get(currentIndexOld);
        }

        return null;
    }

    public InetSocketAddress next(long spinDelay) {
        boolean needToSleep = false;
        InetSocketAddress addr;

        synchronized(this) {
            if (reconfigMode) {
                addr = nextHostInReconfigMode();
                if (addr != null) {
                	currentIndex = serverAddresses.indexOf(addr);
                	return resolve(addr);
                }
                                reconfigMode = false;
                needToSleep = (spinDelay > 0);
            }        
            ++currentIndex;
            if (currentIndex == serverAddresses.size()) {
                currentIndex = 0;
            }            
            addr = serverAddresses.get(currentIndex);
            needToSleep = needToSleep || (currentIndex == lastIndex && spinDelay > 0);
            if (lastIndex == -1) { 
                                lastIndex = 0;
            }
        }
        if (needToSleep) {
            try {
                Thread.sleep(spinDelay);
            } catch (InterruptedException e) {
                LOG.warn("Unexpected exception", e);
            }
        }

        return resolve(addr);
    }

    public synchronized void onConnected() {
        lastIndex = currentIndex;
        reconfigMode = false;
    }

}
