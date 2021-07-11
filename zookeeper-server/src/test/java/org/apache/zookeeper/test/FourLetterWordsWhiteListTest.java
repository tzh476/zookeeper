package org.apache.zookeeper.test;

import java.io.IOException;

import org.apache.zookeeper.TestableZooKeeper;
import org.apache.zookeeper.common.X509Exception.SSLContextException;

import static org.apache.zookeeper.client.FourLetterWordMain.send4LetterWord;

import org.apache.zookeeper.server.command.FourLetterCommands;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FourLetterWordsWhiteListTest extends ClientBase {
    protected static final Logger LOG =
        LoggerFactory.getLogger(FourLetterWordsWhiteListTest.class);

    
    @Test(timeout=30000)
    public void testFourLetterWordsAllDisabledByDefault() throws Exception {
        stopServer();
        FourLetterCommands.resetWhiteList();
        System.setProperty("zookeeper.4lw.commands.whitelist", "stat");
        startServer();

                verifyAllCommandsFail();

        TestableZooKeeper zk = createClient();

        verifyAllCommandsFail();

        zk.getData("/", true, null);

        verifyAllCommandsFail();

        zk.close();

        verifyFuzzyMatch("stat", "Outstanding");
        verifyAllCommandsFail();
    }

    @Test(timeout=30000)
    public void testFourLetterWordsEnableSomeCommands() throws Exception {
        stopServer();
        FourLetterCommands.resetWhiteList();
        System.setProperty("zookeeper.4lw.commands.whitelist", "stat, ruok, isro");
        startServer();
                verifyFuzzyMatch("stat", "Outstanding");
        verifyExactMatch("ruok", "imok");
        verifyExactMatch("isro", "rw");

                verifyExactMatch("conf", generateExpectedMessage("conf"));
        verifyExactMatch("cons", generateExpectedMessage("cons"));
        verifyExactMatch("crst", generateExpectedMessage("crst"));
        verifyExactMatch("dirs", generateExpectedMessage("dirs"));
        verifyExactMatch("dump", generateExpectedMessage("dump"));
        verifyExactMatch("envi", generateExpectedMessage("envi"));
        verifyExactMatch("gtmk", generateExpectedMessage("gtmk"));
        verifyExactMatch("stmk", generateExpectedMessage("stmk"));
        verifyExactMatch("srst", generateExpectedMessage("srst"));
        verifyExactMatch("wchc", generateExpectedMessage("wchc"));
        verifyExactMatch("wchp", generateExpectedMessage("wchp"));
        verifyExactMatch("wchs", generateExpectedMessage("wchs"));
        verifyExactMatch("mntr", generateExpectedMessage("mntr"));
    }

    @Test(timeout=30000)
    public void testISROEnabledWhenReadOnlyModeEnabled() throws Exception {
        stopServer();
        FourLetterCommands.resetWhiteList();
        System.setProperty("zookeeper.4lw.commands.whitelist", "stat");
        System.setProperty("readonlymode.enabled", "true");
        startServer();
        verifyExactMatch("isro", "rw");
        System.clearProperty("readonlymode.enabled");
    }

    @Test(timeout=30000)
    public void testFourLetterWordsInvalidConfiguration() throws Exception {
        stopServer();
        FourLetterCommands.resetWhiteList();
        System.setProperty("zookeeper.4lw.commands.whitelist", "foo bar" +
                " foo,,, " +
                "bar :.,@#$%^&*() , , , , bar, bar, stat,        ");
        startServer();

                verifyAllCommandsFail();
                verifyFuzzyMatch("stat", "Outstanding");
    }

    @Test(timeout=30000)
    public void testFourLetterWordsEnableAllCommandsThroughAsterisk() throws Exception {
        stopServer();
        FourLetterCommands.resetWhiteList();
        System.setProperty("zookeeper.4lw.commands.whitelist", "*");
        startServer();
        verifyAllCommandsSuccess();
    }

    @Test(timeout=30000)
    public void testFourLetterWordsEnableAllCommandsThroughExplicitList() throws Exception {
        stopServer();
        FourLetterCommands.resetWhiteList();
        System.setProperty("zookeeper.4lw.commands.whitelist",
                "ruok, envi, conf, stat, srvr, cons, dump," +
                        "wchs, wchp, wchc, srst, crst, " +
                        "dirs, mntr, gtmk, isro, stmk");
        startServer();
        verifyAllCommandsSuccess();
    }


    private void verifyAllCommandsSuccess() throws Exception {
        verifyExactMatch("ruok", "imok");
        verifyFuzzyMatch("envi", "java.version");
        verifyFuzzyMatch("conf", "clientPort");
        verifyFuzzyMatch("stat", "Outstanding");
        verifyFuzzyMatch("srvr", "Outstanding");
        verifyFuzzyMatch("cons", "queued");
        verifyFuzzyMatch("dump", "Session");
        verifyFuzzyMatch("wchs", "watches");
        verifyFuzzyMatch("wchp", "");
        verifyFuzzyMatch("wchc", "");

        verifyFuzzyMatch("srst", "reset");
        verifyFuzzyMatch("crst", "reset");

        verifyFuzzyMatch("stat", "Outstanding");
        verifyFuzzyMatch("srvr", "Outstanding");
        verifyFuzzyMatch("cons", "queued");
        verifyFuzzyMatch("gtmk", "306");
        verifyFuzzyMatch("isro", "rw");

        TestableZooKeeper zk = createClient();
        String sid = getHexSessionId(zk.getSessionId());

        verifyFuzzyMatch("stat", "queued");
        verifyFuzzyMatch("srvr", "Outstanding");
        verifyFuzzyMatch("cons", sid);
        verifyFuzzyMatch("dump", sid);
        verifyFuzzyMatch("dirs", "size");

        zk.getData("/", true, null);

        verifyFuzzyMatch("stat", "queued");
        verifyFuzzyMatch("srvr", "Outstanding");
        verifyFuzzyMatch("cons", sid);
        verifyFuzzyMatch("dump", sid);

        verifyFuzzyMatch("wchs", "watching 1");
        verifyFuzzyMatch("wchp", sid);
        verifyFuzzyMatch("wchc", sid);
        verifyFuzzyMatch("dirs", "size");
        zk.close();

        verifyExactMatch("ruok", "imok");
        verifyFuzzyMatch("envi", "java.version");
        verifyFuzzyMatch("conf", "clientPort");
        verifyFuzzyMatch("stat", "Outstanding");
        verifyFuzzyMatch("srvr", "Outstanding");
        verifyFuzzyMatch("cons", "queued");
        verifyFuzzyMatch("dump", "Session");
        verifyFuzzyMatch("wchs", "watch");
        verifyFuzzyMatch("wchp", "");
        verifyFuzzyMatch("wchc", "");

        verifyFuzzyMatch("srst", "reset");
        verifyFuzzyMatch("crst", "reset");

        verifyFuzzyMatch("stat", "Outstanding");
        verifyFuzzyMatch("srvr", "Outstanding");
        verifyFuzzyMatch("cons", "queued");
        verifyFuzzyMatch("mntr", "zk_server_state\tstandalone");
        verifyFuzzyMatch("mntr", "num_alive_connections");
        verifyFuzzyMatch("stat", "Connections");
        verifyFuzzyMatch("srvr", "Connections");
        verifyFuzzyMatch("dirs", "size");
    }

    private void verifyAllCommandsFail() throws Exception {
        verifyExactMatch("ruok", generateExpectedMessage("ruok"));
        verifyExactMatch("conf", generateExpectedMessage("conf"));
        verifyExactMatch("cons", generateExpectedMessage("cons"));
        verifyExactMatch("crst", generateExpectedMessage("crst"));
        verifyExactMatch("dirs", generateExpectedMessage("dirs"));
        verifyExactMatch("dump", generateExpectedMessage("dump"));
        verifyExactMatch("envi", generateExpectedMessage("envi"));
        verifyExactMatch("gtmk", generateExpectedMessage("gtmk"));
        verifyExactMatch("stmk", generateExpectedMessage("stmk"));
        verifyExactMatch("srst", generateExpectedMessage("srst"));
        verifyExactMatch("wchc", generateExpectedMessage("wchc"));
        verifyExactMatch("wchp", generateExpectedMessage("wchp"));
        verifyExactMatch("wchs", generateExpectedMessage("wchs"));
        verifyExactMatch("mntr", generateExpectedMessage("mntr"));
        verifyExactMatch("isro", generateExpectedMessage("isro"));

                verifyFuzzyMatch("srvr", "Outstanding");
    }

    private String sendRequest(String cmd) throws IOException, SSLContextException {
      HostPort hpobj = ClientBase.parseHostPortList(hostPort).get(0);
      return send4LetterWord(hpobj.host, hpobj.port, cmd);
    }

    private void verifyFuzzyMatch(String cmd, String expected) throws IOException, SSLContextException {
        String resp = sendRequest(cmd);
        LOG.info("cmd " + cmd + " expected " + expected + " got " + resp);
        Assert.assertTrue(resp.contains(expected));
    }

    private String generateExpectedMessage(String command) {
        return command + " is not executed because it is not in the whitelist.";
    }

    private void verifyExactMatch(String cmd, String expected) throws IOException, SSLContextException {
        String resp = sendRequest(cmd);
        LOG.info("cmd " + cmd + " expected an exact match of " + expected + "; got " + resp);
        Assert.assertTrue(resp.trim().equals(expected));
    }
}
