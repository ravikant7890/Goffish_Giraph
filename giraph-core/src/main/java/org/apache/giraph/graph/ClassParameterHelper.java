package org.apache.giraph.graph;

import java.lang.reflect.ParameterizedType;

/**
 * Created by anirudh on 19/10/16.
 */
public class ClassParameterHelper {
    public static Class getClassParameter(Object o, int parameterIndex) {
        return (Class) ((ParameterizedType)o.getClass().getGenericSuperclass())
                .getActualTypeArguments()[parameterIndex];
    }
}
