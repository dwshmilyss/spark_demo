package com.yiban.spark.core.http;

import com.yiban.spark.core.http.exception.AppException;
import com.yiban.spark.core.http.exception.ExceptionCode;
import okhttp3.Callback;
import okhttp3.ConnectionPool;
import okhttp3.FormBody;
import okhttp3.Headers;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Map;
import java.util.concurrent.TimeUnit;


/**
 * Created by likai.yu on 2020/10/15
 * <p>
 * From io.naza.link.http.AsyncHttpClient
 */
public class AsyncHttpClient extends BaseHttpClient {

    /**
     * 连接超时，单位：毫秒
     */
    private static final int httpClientConnectTimeOut = 60000;

    /**
     * 读超时，单位：毫秒
     */
    private static final int httpClientReadTimeout = 60000;

    /**
     * 写超时，单位：毫秒
     */
    private static final int httpClientWriteTimeout = 60000;

    /**
     * 连接池大小
     */
    private static final int httpClientMaxIdleConnections = 20;

    /**
     * 可用空闲连接过期时间，单位：秒
     */
    private static final int httpClientKeepAliveDuration = 600;


    private static final String APPLICATION_JSON_UTF8_VALUE = "application/json;charset=UTF-8";

    private static final String APPLICATION_FORM_URLENCODED_VALUE = "application/x-www-form-urlencoded";

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncHttpClient.class);

    private static OkHttpClient okHttpClient;

    private static final okhttp3.MediaType JSON = okhttp3.MediaType.parse(APPLICATION_JSON_UTF8_VALUE);

    public static Headers jsonAcceptHeaders() {
        return new Headers.Builder().add(HttpHeaders.ACCEPT, APPLICATION_JSON_UTF8_VALUE).build();
    }

    public static Headers formContentHeaders() {
        return new Headers.Builder()
                .add(HttpHeaders.ACCEPT, APPLICATION_JSON_UTF8_VALUE)
                .add(HttpHeaders.CONTENT_TYPE, APPLICATION_FORM_URLENCODED_VALUE)
                .build();
    }

    static {
        ConnectionPool pool = new ConnectionPool(httpClientMaxIdleConnections, httpClientKeepAliveDuration, TimeUnit.SECONDS);
        okHttpClient = createBuilder(true)
                .connectTimeout(httpClientConnectTimeOut, TimeUnit.MILLISECONDS)
                .readTimeout(httpClientReadTimeout, TimeUnit.MILLISECONDS)
                .writeTimeout(httpClientWriteTimeout, TimeUnit.MILLISECONDS)
                .connectionPool(pool)
                .followRedirects(true)
                .retryOnConnectionFailure(true)
                .build();
        LOGGER.info("AsyncHttpClient created");
    }

    public static void destroy() {
        if (okHttpClient != null) {
            LOGGER.info("Closing AsyncHttpClient...");
            okHttpClient.dispatcher().executorService().shutdown();
            okHttpClient.connectionPool().evictAll();
            LOGGER.info("AsyncHttpClient closed");
        }
    }

    private static OkHttpClient.Builder createBuilder(boolean enableSslValidation) {
        OkHttpClient.Builder builder = new OkHttpClient.Builder();
        if (enableSslValidation) {
            try {
                X509TrustManager disabledTrustManager = new DisableValidationTrustManager();
                TrustManager[] trustManagers = new TrustManager[1];
                trustManagers[0] = disabledTrustManager;
                SSLContext sslContext = SSLContext.getInstance("SSL");
                sslContext.init(null, trustManagers, new java.security.SecureRandom());
                SSLSocketFactory disabledSSLSocketFactory = sslContext.getSocketFactory();
                builder.sslSocketFactory(disabledSSLSocketFactory, disabledTrustManager);
                builder.hostnameVerifier((String s, SSLSession sslSession) -> true);
            } catch (NoSuchAlgorithmException | KeyManagementException e) {
                LOGGER.warn("Error setting SSLSocketFactory in OKHttpClient", e);
            }
        }
        return builder;
    }

    public static class DisableValidationTrustManager implements X509TrustManager {

        @Override
        public void checkClientTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
        }

        @Override
        public void checkServerTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    }

    public static void executeAsync(Request request, Callback callback) {
        okHttpClient.newCall(request).enqueue(callback);
    }

    public static Response execute(Request request) {
        try {
            LOGGER.info("executing sync request [{}]", request.toString());
            Response response = okHttpClient.newCall(request).execute();
            LOGGER.info("get response [{}]", response.toString());
            return response;
        } catch (IOException e) {
            LOGGER.error("Fail to execute request with OkHttpClient", e);
            throw new AppException(ExceptionCode.S_00009, e, e.getMessage());
        }
    }

    public static Request buildPostRequest(String url, Headers headers) {
        Request.Builder builder = new Request.Builder().url(url);
        if (headers != null) {
            builder.headers(headers);
        }
        return builder.method("POST", emptyBody()).build();
    }

    public static Request buildPostRequest(String url, Headers headers, Map<String, String> params) {
        RequestBody body = null;
        if (params != null) {
            body = buildFormBody(params);
        } else {
            body = emptyBody();
        }
        Request.Builder builder = new Request.Builder().url(url);
        if (headers != null) {
            builder.headers(headers);
        }
        return builder.post(body).build();
    }

    public static Request buildPostRequest(String url, Headers headers, String json) {
        RequestBody body;
        if (StringUtils.isNotBlank(json)) {
            body = RequestBody.create(JSON, json);
        } else {
            body = emptyBody();
        }
        Request.Builder builder = new Request.Builder().url(url);
        if (headers != null) {
            builder.headers(headers);
        }
        return builder.post(body).build();
    }

    public static String post(String url) {
        Request request = buildPostRequest(url, null);
        return extractBody(execute(request));
    }

    public static String post(String url, Headers headers) {
        Request request = buildPostRequest(url, headers);
        return extractBody(execute(request));
    }

    public static String post(String url, Map<String, String> params) {
        Request request = buildPostRequest(url, null, params);
        return extractBody(execute(request));
    }

    public static String post(String url, Headers headers, Map<String, String> params) {
        Request request = buildPostRequest(url, headers, params);
        return extractBody(execute(request));
    }

    public static String post(String url, String json) {
        Request request = buildPostRequest(url, null, json);
        return extractBody(execute(request));
    }

    public static byte[] getBytesFromPost(String url, String json) {
        Request request = buildPostRequest(url, null, json);
        return extractBodyToBytes(execute(request));
    }

    public static String post(String url, Headers headers, String json) {
        Request request = buildPostRequest(url, headers, json);
        return extractBody(execute(request));
    }

    public static Request buildPutRequest(String url, Headers headers) {
        Request.Builder builder = new Request.Builder().url(url);
        if (headers != null) {
            builder.headers(headers);
        }
        return builder.method("PUT", emptyBody()).build();
    }

    public static Request buildPutRequest(String url, Headers headers, String json) {
        RequestBody body;
        if (StringUtils.isNotBlank(json)) {
            body = RequestBody.create(JSON, json);
        } else {
            body = emptyBody();
        }
        Request.Builder builder = new Request.Builder().url(url);
        if (headers != null) {
            builder.headers(headers);
        }
        return builder.put(body).build();
    }

    public static Request buildPutRequest(String url, Headers headers, Map<String, String> params) {
        RequestBody body = null;
        if (params != null) {
            body = buildFormBody(params);
        } else {
            body = emptyBody();
        }
        Request.Builder builder = new Request.Builder().url(url);
        if (headers != null) {
            builder.headers(headers);
        }
        return builder.put(body).build();
    }

    public static String put(String url) {
        Request request = buildPutRequest(url, null);
        return extractBody(execute(request));
    }

    public static String put(String url, Headers headers) {
        Request request = buildPutRequest(url, headers);
        return extractBody(execute(request));
    }

    public static String put(String url, Map<String, String> params) {
        Request request = buildPutRequest(url, null, params);
        return extractBody(execute(request));
    }

    public static String put(String url, Headers headers, Map<String, String> params) {
        Request request = buildPutRequest(url, headers, params);
        return extractBody(execute(request));
    }

    public static String put(String url, String json) {
        Request request = buildPutRequest(url, null, json);
        return extractBody(execute(request));
    }

    public static String put(String url, Headers headers, String json) {
        Request request = buildPutRequest(url, headers, json);
        return extractBody(execute(request));
    }

    public static Request buildGetRequest(String url, Headers headers, Map<String, String> params) {
        String uri = parse(url, params);
        Request.Builder builder = new Request.Builder().url(uri);
        if (headers != null) {
            builder.headers(headers);
        }
        return builder.get().build();
    }

    public static String get(String url) {
        Request request = buildGetRequest(url, null, null);
        return extractBody(execute(request));
    }

    public static String get(String url, Headers headers) {
        Request request = buildGetRequest(url, headers, null);
        return extractBody(execute(request));
    }

    public static String get(String url, Map<String, String> params) {
        Request request = buildGetRequest(url, null, params);
        return extractBody(execute(request));
    }

    public static String get(String url, Headers headers, Map<String, String> params) {
        Request request = buildGetRequest(url, headers, params);
        return extractBody(execute(request));
    }

    public static RequestBody buildFormBody(Map<String, String> params) {
        FormBody.Builder builder = new FormBody.Builder();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            if (entry.getKey() == null || entry.getValue() == null) {
                continue;
            }
            builder.add(entry.getKey(), entry.getValue());
        }
        return builder.build();
    }

    public static String extractBody(Response response) {
        String body = null;
        try {
            if (response.body() != null && response.code() == 200) {
                body = response.body().string();
            }
        } catch (IOException e) {
            LOGGER.error("Fail to extract body", e);
            throw new AppException(ExceptionCode.S_00009, e, e.getMessage());
        }
        return body;
    }

    public static byte[] extractBodyToBytes(Response response) {
        byte[] body = null;
        try {
            if (response.body() != null && response.code() == 200) {
                body = response.body().bytes();
            }
        } catch (IOException e) {
            LOGGER.error("Fail to extract body to bytes", e);
            throw new AppException(ExceptionCode.S_00009, e, e.getMessage());
        }
        return body;
    }

    private static RequestBody emptyBody() {
        return RequestBody.create(null, new byte[0]);
    }

}
