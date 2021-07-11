package org.apache.zookeeper.server.quorum.flexible;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Set;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;




public class QuorumHierarchical implements QuorumVerifier {
    private static final Logger LOG = LoggerFactory.getLogger(QuorumHierarchical.class);

    private HashMap<Long, Long> serverWeight = new HashMap<Long, Long>();
    private HashMap<Long, Long> serverGroup = new HashMap<Long, Long>();
    private HashMap<Long, Long> groupWeight = new HashMap<Long, Long>();
    
    private int numGroups = 0;
   
    private Map<Long, QuorumServer> allMembers = new HashMap<Long, QuorumServer>();
    private Map<Long, QuorumServer> participatingMembers = new HashMap<Long, QuorumServer>();
    private Map<Long, QuorumServer> observingMembers = new HashMap<Long, QuorumServer>();
    
    private long version = 0;
    
    public int hashCode() {
         assert false : "hashCode not designed";
         return 42;     }
    
   public boolean equals(Object o){
       if (!(o instanceof QuorumHierarchical)) {
           return false;           
       }       
       QuorumHierarchical qm = (QuorumHierarchical)o;
       if (qm.getVersion() == version) return true;
       if ((allMembers.size()!=qm.getAllMembers().size()) ||
           (serverWeight.size() != qm.serverWeight.size()) ||
           (groupWeight.size() != qm.groupWeight.size()) ||
            (serverGroup.size() != qm.serverGroup.size())) {
           return false;
       }   
       for (QuorumServer qs: allMembers.values()){
           QuorumServer qso = qm.getAllMembers().get(qs.id);
           if (qso == null || !qs.equals(qso)) return false;
       }
       for (Entry<Long, Long> entry : serverWeight.entrySet()) {
           if (!entry.getValue().equals(qm.serverWeight.get(entry.getKey())))
               return false;
       }
       for (Entry<Long, Long> entry : groupWeight.entrySet()) {
           if (!entry.getValue().equals(qm.groupWeight.get(entry.getKey())))
               return false;
       }
       for (Entry<Long, Long> entry : serverGroup.entrySet()) {
           if (!entry.getValue().equals(qm.serverGroup.get(entry.getKey())))
               return false;
       }
       return true;
   }
    
    public QuorumHierarchical(String filename)
    throws ConfigException {
        readConfigFile(filename);    
    }
    
    
    
    public QuorumHierarchical(Properties qp) throws ConfigException {
        parse(qp);
        LOG.info(serverWeight.size() + ", " + serverGroup.size() + ", " + groupWeight.size());
    }
  
    
    public long getWeight(long id){
        return serverWeight.get(id);
    }
    
    
    private void readConfigFile(String filename)
    throws ConfigException{
        File configFile = new File(filename);

        LOG.info("Reading configuration from: " + configFile);

        try {
            if (!configFile.exists()) {
                throw new IllegalArgumentException(configFile.toString()
                        + " file is missing");
            }
    
            Properties cfg = new Properties();
            FileInputStream in = new FileInputStream(configFile);
            try {
                cfg.load(in);
            } finally {
                in.close();
            }
    
            parse(cfg);
        } catch (IOException e) {
            throw new ConfigException("Error processing " + filename, e);
        } catch (IllegalArgumentException e) {
            throw new ConfigException("Error processing " + filename, e);
        }
        
    }
    
    
    
    private void parse(Properties quorumProp) throws ConfigException{
        for (Entry<Object, Object> entry : quorumProp.entrySet()) {
            String key = entry.getKey().toString();
            String value = entry.getValue().toString(); 
            
            if (key.startsWith("server.")) {
                int dot = key.indexOf('.');
                long sid = Long.parseLong(key.substring(dot + 1));
                QuorumServer qs = new QuorumServer(sid, value);
                allMembers.put(Long.valueOf(sid), qs);  
                if (qs.type == LearnerType.PARTICIPANT) 
                   participatingMembers.put(Long.valueOf(sid), qs);
                else {
                   observingMembers.put(Long.valueOf(sid), qs);
                }
            } else if (key.startsWith("group")) {
                int dot = key.indexOf('.');
                long gid = Long.parseLong(key.substring(dot + 1));
                
                numGroups++;
                
                String parts[] = value.split(":");
                for(String s : parts){
                    long sid = Long.parseLong(s);
                    if(serverGroup.containsKey(sid))
                        throw new ConfigException("Server " + sid + "is in multiple groups");
                    else
                        serverGroup.put(sid, gid);
                }
                    
                
            } else if(key.startsWith("weight")) {
                int dot = key.indexOf('.');
                long sid = Long.parseLong(key.substring(dot + 1));
                serverWeight.put(sid, Long.parseLong(value));
            } else if (key.equals("version")){
               version = Long.parseLong(value, 16);
            }        
        }
        
        for (QuorumServer qs: allMembers.values()){
           Long id = qs.id;
           if (qs.type == LearnerType.PARTICIPANT){
               if (!serverGroup.containsKey(id)) 
                   throw new ConfigException("Server " + id + "is not in a group");
               if (!serverWeight.containsKey(id))
                   serverWeight.put(id, (long) 1);
            }
        }
           
           
        computeGroupWeight();
    }
    
    public Map<Long, QuorumServer> getAllMembers() { 
       return allMembers;
    }
    public String toString(){
       StringWriter sw = new StringWriter();
       
       for (QuorumServer member: getAllMembers().values()){            
               String key = "server." + member.id;
            String value = member.toString();
            sw.append(key);
            sw.append('=');
            sw.append(value);
            sw.append('\n');                       
       }
       
       Map<Long, String> groups = new HashMap<Long, String>();
       for (Entry<Long, Long> pair: serverGroup.entrySet()) {
           Long sid = pair.getKey();
           Long gid = pair.getValue();
           String str = groups.get(gid);
           if (str == null) str = sid.toString();
           else str = str.concat(":").concat(sid.toString());
           groups.put(gid, str);
       }
       
       for (Entry<Long, String> pair: groups.entrySet()) {
           Long gid = pair.getKey();
           String key = "group." + gid.toString();
            String value = pair.getValue();
            sw.append(key);
            sw.append('=');
            sw.append(value);
            sw.append('\n');           
       }
   
   
       for (Entry<Long, Long> pair: serverWeight.entrySet()) {
           Long sid = pair.getKey();
           String key = "weight." + sid.toString();
            String value = pair.getValue().toString();
            sw.append(key);
            sw.append('=');
            sw.append(value);
            sw.append('\n');           
       }
       
       sw.append("version=" + Long.toHexString(version));
       
       return sw.toString();        
    }
    
    
    private void computeGroupWeight(){
        for (Entry<Long, Long> entry : serverGroup.entrySet()) {
            Long sid = entry.getKey();
            Long gid = entry.getValue();
            if(!groupWeight.containsKey(gid))
                groupWeight.put(gid, serverWeight.get(sid));
            else {
                long totalWeight = serverWeight.get(sid) + groupWeight.get(gid);
                groupWeight.put(gid, totalWeight);
            }
        }
        
        
        for(long weight: groupWeight.values()){
            LOG.debug("Group weight: " + weight);
            if(weight == ((long) 0)){
                numGroups--;
                LOG.debug("One zero-weight group: " + 1 + ", " + numGroups);
            }
        }
    }
    
    
    public boolean containsQuorum(Set<Long> set){
        HashMap<Long, Long> expansion = new HashMap<Long, Long>();
        
        
        if(set.size() == 0) return false;
        else LOG.debug("Set size: " + set.size());
        
        for(long sid : set){
            Long gid = serverGroup.get(sid);
            if (gid == null) continue;
            if(!expansion.containsKey(gid))
                expansion.put(gid, serverWeight.get(sid));
            else {
                long totalWeight = serverWeight.get(sid) + expansion.get(gid);
                expansion.put(gid, totalWeight);
            }
        }
  
        
        int majGroupCounter = 0;
        for (Entry<Long, Long> entry : expansion.entrySet()) {
            Long gid = entry.getKey();
            LOG.debug("Group info: {}, {}, {}", entry.getValue(), gid, groupWeight.get(gid));
            if (entry.getValue() > (groupWeight.get(gid) / 2))
                majGroupCounter++;
        }

        LOG.debug("Majority group counter: {}, {}", majGroupCounter, numGroups);
        if ((majGroupCounter > (numGroups / 2))){
            LOG.debug("Positive set size: {}", set.size());
            return true;
        } else {
            LOG.debug("Negative set size: {}", set.size());
            return false;
        }
    }
    public Map<Long, QuorumServer> getVotingMembers() {
       return participatingMembers;
   }

   public Map<Long, QuorumServer> getObservingMembers() {
       return observingMembers;
   }

   public long getVersion() {
       return version;
   }          

   public void setVersion(long ver) {
       version = ver;
   }

}
