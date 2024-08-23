package com.wentry.wmq.utils.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.wentry.wmq.domain.registry.partition.PartitionInfo;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @Description:
 * @Author: tangwc
 */
public class JsonUtils {

    static Map<MapperKey, ObjectMapper> ignoreFieldMapper = new ConcurrentHashMap<>();

    static ObjectMapper defaultMapper = new ObjectMapper();

    public static <T> T parseJson(String json, TypeReference<T> valueTypeRef) {
        try {
            return defaultMapper.readValue(json, valueTypeRef);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
    public static <T> T parseJson(String json, Class<T> clz) {
        try {
            return defaultMapper.readValue(json, clz);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    static class MapperKey {
        Class<?> objClz;
        String[] ignoreFields;

        public Class<?> getObjClz() {
            return objClz;
        }

        public MapperKey setObjClz(Class<?> objClz) {
            this.objClz = objClz;
            return this;
        }

        public String[] getIgnoreFields() {
            return ignoreFields;
        }

        public MapperKey setIgnoreFields(String[] ignoreFields) {
            this.ignoreFields = ignoreFields;
            return this;
        }


        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MapperKey mapperKey = (MapperKey) o;
            return Objects.equals(objClz, mapperKey.objClz) && Arrays.equals(ignoreFields, mapperKey.ignoreFields);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(objClz);
            result = 31 * result + Arrays.hashCode(ignoreFields);
            return result;
        }
    }


    /**
     * json 动态忽略指定名的属性
     */
    public static String toJson(Object obj, String... ignoreFields) {
        try {
            // 序列化对象
            return getOrInitMapper(obj.getClass(), ignoreFields).writeValueAsString(obj);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private static ObjectMapper getOrInitMapper(Class<?> aClass, String[] ignoreFields) {
        MapperKey mapperKey = new MapperKey().setObjClz(aClass).setIgnoreFields(ignoreFields);
        ObjectMapper mapper = ignoreFieldMapper.get(mapperKey);
        if (mapper != null) {
            return mapper;
        }
        mapper = new ObjectMapper();
        // 创建一个 SimpleModule 并注册 SerializerModifier
        SimpleModule module = new SimpleModule();
        module.setSerializerModifier(new DynamicFieldSerializerModifier(Arrays.stream(ignoreFields).collect(Collectors.toList())));

        // 注册模块到 ObjectMapper
        mapper.registerModule(module);
        ignoreFieldMapper.put(mapperKey, mapper);

        return mapper;
    }

    public static String toJson(Object obj) {
        if (obj == null) {
            return "{}";
        }
        try {
            return defaultMapper.writerWithDefaultPrettyPrinter().writeValueAsString(obj);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "{}";
    }

    public static void main(String[] args) {
//        long start = System.nanoTime();
//        int times = 199999;
//        for (int i = 0; i < times; i++) {
//            String s = toJson(new JSonUser().setName("jack").setId("id"), "name");
//        }
//        System.out.println("cost" + (System.nanoTime() - start) / times + " ns average");

    }

    static class JSonUser {
        String name;
        String id;

        public String getName() {
            return name;
        }

        public JSonUser setName(String name) {
            this.name = name;
            return this;
        }

        public String getId() {
            return id;
        }

        public JSonUser setId(String id) {
            this.id = id;
            return this;
        }
    }

}
