package de.rngcntr.gremlin.optimize.util;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;

import java.util.HashMap;
import java.util.Map;

public class TraverserUtils {

    public static <E> Map<String, Object> mapHistory(Traverser<E> t) {
        Map<String, Object> returnMap = new HashMap();
        t.path().forEach((o,ls) -> {
            ls.forEach(l -> returnMap.put(l, o));
        });

        if (t.get() instanceof Map) {
            Map<?,?> content = (Map<?,?>) t.get();
            content.entrySet().forEach(e -> {
                if (e.getKey() instanceof String) {
                    returnMap.put((String) e.getKey(), e.getValue());
                }
            });
        }

        return returnMap;
    }
}
