package org.apache.zookeeper.server.util;

import java.io.IOException;
import java.io.StringReader;
import java.util.Properties;
import java.util.Map.Entry;

import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;


public class ConfigUtils {
    static public String getClientConfigStr(String configData) {
        Properties props = new Properties();        
        try {
          props.load(new StringReader(configData));
        } catch (IOException e) {
            e.printStackTrace();
            return "";
        }
        StringBuffer sb = new StringBuffer();
        boolean first = true;
        String version = "";
        for (Entry<Object, Object> entry : props.entrySet()) {
             String key = entry.getKey().toString().trim();
             String value = entry.getValue().toString().trim();
             if (key.equals("version")) version = value;
             if (!key.startsWith("server.")) continue;           
             QuorumPeer.QuorumServer qs;
             try {
               qs = new QuorumPeer.QuorumServer(-1, value);
             } catch (ConfigException e) {              
                    e.printStackTrace();
                    continue;
             }
             if (!first) sb.append(",");
             else first = false;
             if (null != qs.clientAddr) {
                 sb.append(qs.clientAddr.getHostString()
                         + ":" + qs.clientAddr.getPort());
             }
        }
        return version + " " + sb.toString();
    }

    
    public static String[] getHostAndPort(String s)
            throws ConfigException
    {
        if (s.startsWith("[")) {
            int i = s.indexOf("]:");
            if (i < 0) {
                throw new ConfigException(s + " starts with '[' but has no matching ']:'");
            }

            String[] sa = s.substring(i + 2).split(":");
            String[] nsa = new String[sa.length + 1];
            nsa[0] = s.substring(1, i);
            System.arraycopy(sa, 0, nsa, 1, sa.length);

            return nsa;
        } else {
            return s.split(":");
        }
    }
}
