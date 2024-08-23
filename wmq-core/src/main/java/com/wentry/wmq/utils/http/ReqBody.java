package com.wentry.wmq.utils.http;

import com.fasterxml.jackson.core.type.TypeReference;
import com.wentry.wmq.utils.json.JsonUtils;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Description:
 * @Author: tangwc
 */
public class ReqBody implements Serializable {

    private static final long serialVersionUID = -5128498042911955980L;

    private final Map<String, Object> data = new ConcurrentHashMap<>();

    public ReqBody() {
    }

    public ReqBody(Map<String, Object> reqbody) {
        if (reqbody != null) {
            data.putAll(reqbody);
        }
    }

    public ReqBody addData(String k, Object v) {
        data.put(k, v);
        return this;
    }

    public <T> T getData(String k, TypeReference<T> valueTypeRef) {
        Object o = data.get(k);
        if (o == null) {
            return null;
        }
        return JsonUtils.parseJson(JsonUtils.toJson(o), valueTypeRef);
    }

    public <T> T getData(String k, Class<T> clz) {
        Object o = data.get(k);
        if (o == null) {
            return null;
        }
        return JsonUtils.parseJson(JsonUtils.toJson(o), clz);
    }

    public Map<String, Object> getData() {
        return data;
    }
}
