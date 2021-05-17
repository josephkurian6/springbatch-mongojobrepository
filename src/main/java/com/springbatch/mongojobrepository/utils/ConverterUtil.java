package com.springbatch.mongojobrepository.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ConverterUtil {

    private ConverterUtil() {
        throw new IllegalStateException("Utility class");
    }

    public static Map<String, Object> convertEntryToMap(Set<Map.Entry<String, Object>> map) {
        Map<String, Object> convertData = new HashMap<>();
        if(map!=null && !map.isEmpty()) {
            for (Map.Entry<String, Object> entry : map) {
                convertData.put(entry.getKey(), entry.getValue());
            }
        }
        return convertData;
    }
}
