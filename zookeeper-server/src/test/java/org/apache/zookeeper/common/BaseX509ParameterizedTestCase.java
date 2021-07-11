package org.apache.zookeeper.common;

import java.io.File;
import java.io.IOException;
import java.security.Security;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.test.ClientBase;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.AfterClass;
import org.junit.BeforeClass;


public abstract class BaseX509ParameterizedTestCase extends ZKTestCase {
    
    public static Collection<Object[]> defaultParams() {
        ArrayList<Object[]> result = new ArrayList<>();
        int paramIndex = 0;
        for (X509KeyType caKeyType : X509KeyType.values()) {
            for (X509KeyType certKeyType : X509KeyType.values()) {
                for (String keyPassword : new String[]{"", "pa$$w0rd"}) {
                    result.add(new Object[]{caKeyType, certKeyType, keyPassword, paramIndex++});
                }
            }
        }
        return result;
    }

    
    protected static Map<Integer, X509TestContext> cachedTestContexts;
    protected static File tempDir;

    protected X509TestContext x509TestContext;

    @BeforeClass
    public static void setUpBaseClass() throws Exception {
        Security.addProvider(new BouncyCastleProvider());
        cachedTestContexts = new HashMap<>();
        tempDir = ClientBase.createEmptyTestDir();
    }

    @AfterClass
    public static void cleanUpBaseClass() {
        Security.removeProvider("BC");
        cachedTestContexts.clear();
        cachedTestContexts = null;
        try {
            FileUtils.deleteDirectory(tempDir);
        } catch (IOException e) {
                    }
    }

    
    protected BaseX509ParameterizedTestCase(
            Integer paramIndex,
            java.util.function.Supplier<X509TestContext> contextSupplier) {
        if (cachedTestContexts.containsKey(paramIndex)) {
            x509TestContext = cachedTestContexts.get(paramIndex);
        } else {
            x509TestContext = contextSupplier.get();
            cachedTestContexts.put(paramIndex, x509TestContext);
        }
    }
}
