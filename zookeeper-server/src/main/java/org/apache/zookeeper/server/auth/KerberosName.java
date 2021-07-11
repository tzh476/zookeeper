package org.apache.zookeeper.server.auth;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.zookeeper.server.util.KerberosUtil;


public class KerberosName {
  
  private final String serviceName;
  
  private final String hostName;
  
  private final String realm;

  
  private static final Pattern nameParser = 
    Pattern.compile("([^/@]*)(/([^/@]*))?@([^/@]*)");

  
  private static Pattern parameterPattern = 
    Pattern.compile("([^$]*)(\\$(\\d*))?");

  
  private static final Pattern ruleParser =
    Pattern.compile("\\s*((DEFAULT)|(RULE:\\[(\\d*):([^\\]]*)](\\(([^)]*)\\))?"+
                    "(s/([^/]*)/([^/]*)/(g)?)?))");
  
  
  private static final Pattern nonSimplePattern = Pattern.compile("[/@]");
  
  
  private static List<Rule> rules;

  private static String defaultRealm;
  
  static {
    try {
      defaultRealm = KerberosUtil.getDefaultRealm();
    } catch (Exception ke) {
      if ((System.getProperty("zookeeper.requireKerberosConfig") != null) &&
          (System.getProperty("zookeeper.requireKerberosConfig").equals("true"))) {
        throw new IllegalArgumentException("Can't get Kerberos configuration",ke);
      }
      else
        defaultRealm="";
    }
    try {
                              setConfiguration();
    }
    catch (IOException e) {
      throw new IllegalArgumentException("Could not configure Kerberos principal name mapping.");
    }
  }

  
  public KerberosName(String name) {
    Matcher match = nameParser.matcher(name);
    if (!match.matches()) {
      if (name.contains("@")) {
        throw new IllegalArgumentException("Malformed Kerberos name: " + name);
      } else {
        serviceName = name;
        hostName = null;
        realm = null;
      }
    } else {
      serviceName = match.group(1);
      hostName = match.group(3);
      realm = match.group(4);
    }
  }

  
  public String getDefaultRealm() {
    return defaultRealm;
  }

  
  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append(serviceName);
    if (hostName != null) {
      result.append('/');
      result.append(hostName);
    }
    if (realm != null) {
      result.append('@');
      result.append(realm);
    }
    return result.toString();
  }

  
  public String getServiceName() {
    return serviceName;
  }

  
  public String getHostName() {
    return hostName;
  }
  
  
  public String getRealm() {
    return realm;
  }
  
  
  private static class Rule {
    private final boolean isDefault;
    private final int numOfComponents;
    private final String format;
    private final Pattern match;
    private final Pattern fromPattern;
    private final String toPattern;
    private final boolean repeat;

    Rule() {
      isDefault = true;
      numOfComponents = 0;
      format = null;
      match = null;
      fromPattern = null;
      toPattern = null;
      repeat = false;
    }

    Rule(int numOfComponents, String format, String match, String fromPattern,
         String toPattern, boolean repeat) {
      isDefault = false;
      this.numOfComponents = numOfComponents;
      this.format = format;
      this.match = match == null ? null : Pattern.compile(match);
      this.fromPattern = 
        fromPattern == null ? null : Pattern.compile(fromPattern);
      this.toPattern = toPattern;
      this.repeat = repeat;
    }
    
    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder();
      if (isDefault) {
        buf.append("DEFAULT");
      } else {
        buf.append("RULE:[");
        buf.append(numOfComponents);
        buf.append(':');
        buf.append(format);
        buf.append(']');
        if (match != null) {
          buf.append('(');
          buf.append(match);
          buf.append(')');
        }
        if (fromPattern != null) {
          buf.append("s/");
          buf.append(fromPattern);
          buf.append('/');
          buf.append(toPattern);
          buf.append('/');
          if (repeat) {
            buf.append('g');
          }
        }
      }
      return buf.toString();
    }
    
    
    static String replaceParameters(String format, 
                                    String[] params) throws BadFormatString {
      Matcher match = parameterPattern.matcher(format);
      int start = 0;
      StringBuilder result = new StringBuilder();
      while (start < format.length() && match.find(start)) {
        result.append(match.group(1));
        String paramNum = match.group(3);
        if (paramNum != null) {
          try {
            int num = Integer.parseInt(paramNum);
            if (num < 0 || num > params.length) {
              throw new BadFormatString("index " + num + " from " + format +
                                        " is outside of the valid range 0 to " +
                                        (params.length - 1));
            }
            result.append(params[num]);
          } catch (NumberFormatException nfe) {
            throw new BadFormatString("bad format in username mapping in " + 
                                      paramNum, nfe);
          }
          
        }
        start = match.end();
      }
      return result.toString();
    }

    
    static String replaceSubstitution(String base, Pattern from, String to, 
                                      boolean repeat) {
      Matcher match = from.matcher(base);
      if (repeat) {
        return match.replaceAll(to);
      } else {
        return match.replaceFirst(to);
      }
    }

    
    String apply(String[] params) throws IOException {
      String result = null;
      if (isDefault) {
        if (defaultRealm.equals(params[0])) {
          result = params[1];
        }
      } else if (params.length - 1 == numOfComponents) {
        String base = replaceParameters(format, params);
        if (match == null || match.matcher(base).matches()) {
          if (fromPattern == null) {
            result = base;
          } else {
            result = replaceSubstitution(base, fromPattern, toPattern,  repeat);
          }
        }
      }
      if (result != null && nonSimplePattern.matcher(result).find()) {
        throw new NoMatchingRule("Non-simple name " + result +
                                 " after auth_to_local rule " + this);
      }
      return result;
    }
  }

  static List<Rule> parseRules(String rules) {
    List<Rule> result = new ArrayList<Rule>();
    String remaining = rules.trim();
    while (remaining.length() > 0) {
      Matcher matcher = ruleParser.matcher(remaining);
      if (!matcher.lookingAt()) {
        throw new IllegalArgumentException("Invalid rule: " + remaining);
      }
      if (matcher.group(2) != null) {
        result.add(new Rule());
      } else {
        result.add(new Rule(Integer.parseInt(matcher.group(4)),
                            matcher.group(5),
                            matcher.group(7),
                            matcher.group(9),
                            matcher.group(10),
                            "g".equals(matcher.group(11))));
      }
      remaining = remaining.substring(matcher.end());
    }
    return result;
  }

  
  public static void setConfiguration() throws IOException {
    String ruleString = System.getProperty("zookeeper.security.auth_to_local", "DEFAULT");
    rules = parseRules(ruleString);
  }

  @SuppressWarnings("serial")
  public static class BadFormatString extends IOException {
    BadFormatString(String msg) {
      super(msg);
    }
    BadFormatString(String msg, Throwable err) {
      super(msg, err);
    }
  }

  @SuppressWarnings("serial")
  public static class NoMatchingRule extends IOException {
    NoMatchingRule(String msg) {
      super(msg);
    }
  }

  
  public String getShortName() throws IOException {
    String[] params;
    if (hostName == null) {
            if (realm == null) {
        return serviceName;
      }
      params = new String[]{realm, serviceName};
    } else {
      params = new String[]{realm, serviceName, hostName};
    }
    for(Rule r: rules) {
      String result = r.apply(params);
      if (result != null) {
        return result;
      }
    }
    throw new NoMatchingRule("No rules applied to " + toString());
  }

  static void printRules() throws IOException {
    int i = 0;
    for(Rule r: rules) {
      System.out.println(++i + " " + r);
    }
  }

  public static void main(String[] args) throws Exception {
    for(String arg: args) {
      KerberosName name = new KerberosName(arg);
      System.out.println("Name: " + name + " to " + name.getShortName());
    }
  }
}
