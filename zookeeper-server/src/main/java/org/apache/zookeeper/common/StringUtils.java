package org.apache.zookeeper.common;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;

public class StringUtils {

    private StringUtils() {}

    
    public static List<String> split(String value, String separator) {
        String[] splits = value.split(separator);
        List<String> results = new ArrayList<String>();
        for (int i = 0; i < splits.length; i++) {
            splits[i] = splits[i].trim();
            if (splits[i].length() > 0) {
               results.add(splits[i]);
            }
        }
        return Collections.unmodifiableList(results);
    }
    
     
    public static String joinStrings(List<String> list, String delim)
    {
        if (list == null)
            return null;

       StringBuilder builder = new StringBuilder(list.get(0));
        for (String s : list.subList(1, list.size())) {
            builder.append(delim).append(s);
        }

        return builder.toString();
    }
}
