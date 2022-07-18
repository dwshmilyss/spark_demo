package com.yiban.spark.core.http;

import com.yiban.spark.core.http.exception.AppException;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by wangding on 22/05/2017.
 */
public class BaseHttpClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseHttpClient.class);

    private final static String EMPTY_STRING = "";
    private final static String SYMBOL_MARK = "?";
    private final static String SYMBOL_CONNECTOR = "&";
    private final static String SYMBOL_EQUAL = "=";

    /**
     * 参数转换
     */
    protected static List<NameValuePair> map2List(Map<String, String> params) {
        List<NameValuePair> list = new ArrayList<>();
        if (params != null && params.size() > 0) {
            params.forEach((k, v) -> {
                list.add(new BasicNameValuePair(k, v == null ? EMPTY_STRING : v));
            });
        }
        return list;
    }

    /**
     * Url追加参数
     */
    protected static String parse(String url, Map<String, String> params) {
        if (params == null || params.size() == 0) {
            return url;
        }
        StringBuilder builder = new StringBuilder(url);
        builder.append(SYMBOL_MARK);
        params.forEach((k, v) -> {
            try {
                builder.append(k).append(SYMBOL_EQUAL).append(URLEncoder.encode(v == null ? EMPTY_STRING : v, "UTF-8")).append(SYMBOL_CONNECTOR);
            } catch (UnsupportedEncodingException e) {
                throw new AppException("Parse Url error", e);
            }
        });
        String parse = builder.toString();
        return parse.substring(0, parse.length() - 1);
    }

}
