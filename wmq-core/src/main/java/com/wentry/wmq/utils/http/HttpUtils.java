package com.wentry.wmq.utils.http;

import com.wentry.wmq.utils.json.JsonUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * @Description:
 * @Author: tangwc
 */
public class HttpUtils {

    private static final Logger log = LoggerFactory.getLogger(HttpUtils.class);

    private static final CloseableHttpClient httpClient = HttpClients.createDefault();

    public static <T> T get(String url, Map<String, String> query, Class<T> responseType) {
        StringBuilder fullUrl = new StringBuilder(url);
        if (MapUtils.isNotEmpty(query)) {
            // 添加查询参数前的问号
            fullUrl.append("?");
            query.forEach((key, value) -> fullUrl.append(key).append("=").append(value).append("&"));
            // 删除最后一个多余的"&"
            fullUrl.deleteCharAt(fullUrl.length() - 1);
        }

        HttpGet request = new HttpGet(fullUrl.toString());

        try (CloseableHttpResponse response = httpClient.execute(request)) {
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode >= 200 && statusCode < 300) {
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    String responseString = EntityUtils.toString(entity);
                    return JsonUtils.parseJson(responseString, responseType);
                } else {
                    throw new RuntimeException("Response entity is null.");
                }
            } else {
                throw new RuntimeException("Unexpected response status: " + statusCode);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error executing GET request", e);
        }
    }

    public static String get(String url) {
        // 创建 HttpGet 实例
        HttpGet request = new HttpGet(url);

        try (CloseableHttpResponse response = httpClient.execute(request)) {
            // 获取响应状态码和内容
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode >= 200 && statusCode < 300) {
                return EntityUtils.toString(response.getEntity());
            } else {
                throw new RuntimeException("Unexpected response status: " + statusCode);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error executing GET request", e);
        }
    }

    public static <REQ,RESP> RESP post(String url, REQ req, Class<RESP> responseType) {
        HttpPost postRequest = new HttpPost(url);
        RESP resp = null;
        String msg = null;
        try {
            // 设置请求体为JSON格式
            HttpEntity entity = new StringEntity(JsonUtils.toJson(req), "UTF-8");
            postRequest.setEntity(entity);

            // 设置Content-Type为application/json
            postRequest.setHeader("Content-Type", "application/json");

            try (CloseableHttpResponse response = httpClient.execute(postRequest)) {
                int statusCode = response.getStatusLine().getStatusCode();
                if (statusCode >= 200 && statusCode < 300) {
                    if (responseType == null) {
                        return null;
                    }
                    String responseString = EntityUtils.toString(response.getEntity());
                    return (resp = JsonUtils.parseJson(responseString, responseType)); // 假设此方法将JSON字符串转换为目标类型
                } else {
                    msg = "Unexpected response status: " + statusCode;
                    throw new RuntimeException("Unexpected response status: " + statusCode);
                }
            }
        } catch (Exception e) {
            msg = "Error executing POST request:" + e.getMessage();
            throw new RuntimeException("Error executing POST request", e);
        } finally {
//            log.info("post url:{}, req:{}, resp:{}, msg:{}", url, JsonUtils.toJson(req), JsonUtils.toJson(resp), JsonUtils.toJson(msg));
        }
    }

    public static <T> T post(String url, Map<String, Object> reqbody, Class<T> responseType) {
        return post(url, new ReqBody(reqbody), responseType);
    }
}
