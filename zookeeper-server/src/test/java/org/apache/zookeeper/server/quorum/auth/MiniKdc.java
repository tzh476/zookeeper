package org.apache.zookeeper.server.quorum.auth;
import org.apache.kerby.kerberos.kerb.KrbException;
import org.apache.kerby.kerberos.kerb.server.KdcConfigKey;
import org.apache.kerby.kerberos.kerb.server.SimpleKdcServer;
import org.apache.kerby.util.IOUtil;
import org.apache.kerby.util.NetworkUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;



public class MiniKdc {

    public static final String JAVA_SECURITY_KRB5_CONF =
            "java.security.krb5.conf";
    public static final String SUN_SECURITY_KRB5_DEBUG =
            "sun.security.krb5.debug";

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.out.println("Arguments: <WORKDIR> <MINIKDCPROPERTIES> " +
                    "<KEYTABFILE> [<PRINCIPALS>]+");
            System.exit(1);
        }
        File workDir = new File(args[0]);
        if (!workDir.exists()) {
            throw new RuntimeException("Specified work directory does not exists: "
                    + workDir.getAbsolutePath());
        }
        Properties conf = createConf();
        File file = new File(args[1]);
        if (!file.exists()) {
            throw new RuntimeException("Specified configuration does not exists: "
                    + file.getAbsolutePath());
        }
        Properties userConf = new Properties();
        InputStreamReader r = null;
        try {
            r = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8);
            userConf.load(r);
        } finally {
            if (r != null) {
                r.close();
            }
        }
        for (Map.Entry<?, ?> entry : userConf.entrySet()) {
            conf.put(entry.getKey(), entry.getValue());
        }
        final MiniKdc miniKdc = new MiniKdc(conf, workDir);
        miniKdc.start();
        File krb5conf = new File(workDir, "krb5.conf");
        if (miniKdc.getKrb5conf().renameTo(krb5conf)) {
            File keytabFile = new File(args[2]).getAbsoluteFile();
            String[] principals = new String[args.length - 3];
            System.arraycopy(args, 3, principals, 0, args.length - 3);
            miniKdc.createPrincipal(keytabFile, principals);
            System.out.println();
            System.out.println("Standalone MiniKdc Running");
            System.out.println("---------------------------------------------------");
            System.out.println("  Realm           : " + miniKdc.getRealm());
            System.out.println("  Running at      : " + miniKdc.getHost() + ":" +
                    miniKdc.getHost());
            System.out.println("  krb5conf        : " + krb5conf);
            System.out.println();
            System.out.println("  created keytab  : " + keytabFile);
            System.out.println("  with principals : " + Arrays.asList(principals));
            System.out.println();
            System.out.println(" Do <CTRL-C> or kill <PID> to stop it");
            System.out.println("---------------------------------------------------");
            System.out.println();
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    miniKdc.stop();
                }
            });
        } else {
            throw new RuntimeException("Cannot rename KDC's krb5conf to "
                    + krb5conf.getAbsolutePath());
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(MiniKdc.class);

    public static final String ORG_NAME = "org.name";
    public static final String ORG_DOMAIN = "org.domain";
    public static final String KDC_BIND_ADDRESS = "kdc.bind.address";
    public static final String KDC_PORT = "kdc.port";
    public static final String INSTANCE = "instance";
    public static final String MAX_TICKET_LIFETIME = "max.ticket.lifetime";
    public static final String MAX_RENEWABLE_LIFETIME = "max.renewable.lifetime";
    public static final String TRANSPORT = "transport";
    public static final String DEBUG = "debug";

    private static final Set<String> PROPERTIES = new HashSet<String>();
    private static final Properties DEFAULT_CONFIG = new Properties();

    static {
        PROPERTIES.add(ORG_NAME);
        PROPERTIES.add(ORG_DOMAIN);
        PROPERTIES.add(KDC_BIND_ADDRESS);
        PROPERTIES.add(KDC_BIND_ADDRESS);
        PROPERTIES.add(KDC_PORT);
        PROPERTIES.add(INSTANCE);
        PROPERTIES.add(TRANSPORT);
        PROPERTIES.add(MAX_TICKET_LIFETIME);
        PROPERTIES.add(MAX_RENEWABLE_LIFETIME);

        DEFAULT_CONFIG.setProperty(KDC_BIND_ADDRESS, "localhost");
        DEFAULT_CONFIG.setProperty(KDC_PORT, "0");
        DEFAULT_CONFIG.setProperty(INSTANCE, "DefaultKrbServer");
        DEFAULT_CONFIG.setProperty(ORG_NAME, "EXAMPLE");
        DEFAULT_CONFIG.setProperty(ORG_DOMAIN, "COM");
        DEFAULT_CONFIG.setProperty(TRANSPORT, "TCP");
        DEFAULT_CONFIG.setProperty(MAX_TICKET_LIFETIME, "86400000");
        DEFAULT_CONFIG.setProperty(MAX_RENEWABLE_LIFETIME, "604800000");
        DEFAULT_CONFIG.setProperty(DEBUG, "false");
    }

    
    public static Properties createConf() {
        return (Properties) DEFAULT_CONFIG.clone();
    }

    private Properties conf;
    private SimpleKdcServer simpleKdc;
    private int port;
    private String realm;
    private File workDir;
    private File krb5conf;
    private String transport;
    private boolean krb5Debug;

    public void setTransport(String transport) {
        this.transport = transport;
    }
    
    public MiniKdc(Properties conf, File workDir) throws Exception {
        if (!conf.keySet().containsAll(PROPERTIES)) {
            Set<String> missingProperties = new HashSet<String>(PROPERTIES);
            missingProperties.removeAll(conf.keySet());
            throw new IllegalArgumentException("Missing configuration properties: "
                    + missingProperties);
        }
        this.workDir = new File(workDir, Long.toString(System.currentTimeMillis()));
        if (!this.workDir.exists()
                && !this.workDir.mkdirs()) {
            throw new RuntimeException("Cannot create directory " + this.workDir);
        }
        LOG.info("Configuration:");
        LOG.info("---------------------------------------------------------------");
        for (Map.Entry<?, ?> entry : conf.entrySet()) {
            LOG.info("  {}: {}", entry.getKey(), entry.getValue());
        }
        LOG.info("---------------------------------------------------------------");
        this.conf = conf;
        port = Integer.parseInt(conf.getProperty(KDC_PORT));
        String orgName= conf.getProperty(ORG_NAME);
        String orgDomain = conf.getProperty(ORG_DOMAIN);
        realm = orgName.toUpperCase(Locale.ENGLISH) + "."
                + orgDomain.toUpperCase(Locale.ENGLISH);
    }

    
    public int getPort() {
        return port;
    }

    
    public String getHost() {
        return conf.getProperty(KDC_BIND_ADDRESS);
    }

    
    public String getRealm() {
        return realm;
    }

    public File getKrb5conf() {
        krb5conf = new File(System.getProperty(JAVA_SECURITY_KRB5_CONF));
        return krb5conf;
    }

    
    public synchronized void start() throws Exception {
        if (simpleKdc != null) {
            throw new RuntimeException("Already started");
        }
        simpleKdc = new SimpleKdcServer();
        prepareKdcServer();
        simpleKdc.init();
        resetDefaultRealm();
        simpleKdc.start();
        LOG.info("MiniKdc stated.");
    }

    private void resetDefaultRealm() throws IOException {
        InputStream templateResource = new FileInputStream(
                getKrb5conf().getAbsolutePath());
        String content = IOUtil.readInput(templateResource);
        content = content.replaceAll("default_realm = .*\n",
                "default_realm = " + getRealm() + "\n");
        IOUtil.writeFile(content, getKrb5conf());
    }

    private void prepareKdcServer() throws Exception {
                simpleKdc.setWorkDir(workDir);
        simpleKdc.setKdcHost(getHost());
        simpleKdc.setKdcRealm(realm);
        if (transport == null) {
            transport = conf.getProperty(TRANSPORT);
        }
        if (port == 0) {
            port = NetworkUtil.getServerPort();
        }
        if (transport != null) {
            if (transport.trim().equals("TCP")) {
                simpleKdc.setKdcTcpPort(port);
                simpleKdc.setAllowUdp(false);
            } else if (transport.trim().equals("UDP")) {
                simpleKdc.setKdcUdpPort(port);
                simpleKdc.setAllowTcp(false);
            } else {
                throw new IllegalArgumentException("Invalid transport: " + transport);
            }
        } else {
            throw new IllegalArgumentException("Need to set transport!");
        }
        simpleKdc.getKdcConfig().setString(KdcConfigKey.KDC_SERVICE_NAME,
                conf.getProperty(INSTANCE));
        if (conf.getProperty(DEBUG) != null) {
            krb5Debug = getAndSet(SUN_SECURITY_KRB5_DEBUG, conf.getProperty(DEBUG));
        }
    }

    
    public synchronized void stop() {
        if (simpleKdc != null) {
            try {
                simpleKdc.stop();
            } catch (KrbException e) {
                e.printStackTrace();
            } finally {
                if(conf.getProperty(DEBUG) != null) {
                    System.setProperty(SUN_SECURITY_KRB5_DEBUG,
                            Boolean.toString(krb5Debug));
                }
            }
        }
        delete(workDir);
        try {
                        Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        LOG.info("MiniKdc stopped.");
    }

    private void delete(File f) {
        if (f.isFile()) {
            if (! f.delete()) {
                LOG.warn("WARNING: cannot delete file " + f.getAbsolutePath());
            }
        } else {
            for (File c: f.listFiles()) {
                delete(c);
            }
            if (! f.delete()) {
                LOG.warn("WARNING: cannot delete directory " + f.getAbsolutePath());
            }
        }
    }

    
    public synchronized void createPrincipal(String principal, String password)
            throws Exception {
        simpleKdc.createPrincipal(principal, password);
    }

    
    public synchronized void createPrincipal(File keytabFile,
                                             String ... principals)
            throws Exception {
        simpleKdc.createPrincipals(principals);
        if (keytabFile.exists() && !keytabFile.delete()) {
            LOG.error("Failed to delete keytab file: " + keytabFile);
        }
        for (String principal : principals) {
            simpleKdc.getKadmin().exportKeytab(keytabFile, principal);
        }
    }

    
    private boolean getAndSet(String sysprop, String debug) {
        boolean old = Boolean.getBoolean(sysprop);
        System.setProperty(sysprop, debug);
        return old;
    }
}