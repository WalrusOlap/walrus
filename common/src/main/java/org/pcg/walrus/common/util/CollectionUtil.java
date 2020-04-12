package org.pcg.walrus.common.util;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.json.simple.JSONObject;

import com.google.common.collect.Maps;

public class CollectionUtil {

    /**
     * return defaultVal if map do not contains key
     */
    public static String getOrDefault(Map<String, String> map, String key, String defaultVal) {
        return map.get(key) == null ? defaultVal : map.get(key);
    }

   /**
     * convert json to map
     */
    public static Map<String, Object> jsonToMap(JSONObject json) {
        Map<String, Object> map = Maps.newHashMap();
        for (Object key : json.keySet())
            map.put(key.toString(), json.get(key));
        return map;
    }

    /**
     * return elemet in b that not in a
     */
    public static Set<String> diffSets(Set<String> a, Set<String> b) {
        Set<String> set = new HashSet<String>();
        if (b == null)
            return set;
        if (a == null)
            return b;
        for (String k : b)
            if (!k.isEmpty() && !a.contains(k))
                set.add(k);
        return set;
    }

    /**
     * return intersection of set a and b
     */
    public static Set<String> intersection(Set<String> a, Set<String> b) {
        Set<String> set = new HashSet<String>();
        if (a == null || b == null)
            return set;
        for (Object k : CollectionUtils.intersection(a, b))
            set.add(k.toString());
        return set;
    }
}
