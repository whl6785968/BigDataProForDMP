package utils;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.LinkedHashMap;
import java.util.Map;

public class GeoUtils {
    public static String getBusiness(String latAndLong) throws IOException {
        LinkedHashMap<String, String> paramMap = new LinkedHashMap<String, String>();

        paramMap.put("callback","renderReverse");
        paramMap.put("location",latAndLong);
        paramMap.put("output","json");
        paramMap.put("pois","1");
        paramMap.put("ak","uzs5G9t190lBpWmcohwIHFcgPKmdTnGb");

        String paramStr = toQueryString(paramMap);
        System.out.println("paramStr = " + paramStr);
        // 对paramsStr前面拼接上/geocoder/v2/?，后面直接拼接yoursk得到/geocoder/v2/?address=%E7%99%BE%E5%BA%A6%E5%A4%A7%E5%8E%A6&output=json&ak=yourakyoursk
        String wholeStr = new String("/geocoder/v2/?" + paramStr + "AZxGLoATKDeGsAAVXB2nL7fUSX6OzZMq");
        String tempStr = URLEncoder.encode(wholeStr, "UTF-8");

        String sn = MD5(tempStr);
        System.out.println("sn = " + sn);
        String business = null;

        HttpClient httpClient = new HttpClient();
        //http://api.map.baidu.com/geocoder/v2/?callback=renderReverse&location=35.658651%2C139.745415&output=json&pois=1&ak=uzs5G9t190lBpWmcohwIHFcgPKmdTnGb&sn=5c3edfea739073f1f3819a565876bc9a
        GetMethod getMethod = new GetMethod("http://api.map.baidu.com/geocoder/v2/?"+paramStr+"&sn="+sn);
        int code = httpClient.executeMethod(getMethod);
        System.out.println("code = " + code);
        if(code == 200) {
            String body = getMethod.getResponseBodyAsString();
            getMethod.releaseConnection();

            if(body.startsWith("renderReverse&&renderReverse(")){
                String replaced = body.replace("renderReverse&&renderReverse(","");
                replaced = replaced.substring(0,replaced.lastIndexOf(")"));
                System.out.println("replaced = " + replaced);

                JSONObject jsonObject = JSON.parseObject(replaced);

                JSONObject result = jsonObject.getJSONObject("result");

                business = result.getString("business");
                System.out.println("business = " + business);

                if(StringUtils.isEmpty(business)){
                    JSONArray pois = jsonObject.getJSONArray("pois");

                    if(pois != null && pois.size()>0)
                    {
                        business=pois.getJSONObject(0).getString("tag");
                    }
                }
            }
        }

        return business;

    }

    public static String toQueryString(Map<?, ?> data)
            throws UnsupportedEncodingException {
        StringBuffer queryString = new StringBuffer();
        for (Map.Entry<?, ?> pair : data.entrySet()) {
            queryString.append(pair.getKey() + "=");
            queryString.append(URLEncoder.encode((String) pair.getValue(),
                    "UTF-8") + "&");
        }
        if (queryString.length() > 0) {
            queryString.deleteCharAt(queryString.length() - 1);
        }
        return queryString.toString();
    }

    public static String MD5(String md5) {
        try {
            java.security.MessageDigest md = java.security.MessageDigest
                    .getInstance("MD5");
            byte[] array = md.digest(md5.getBytes());
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < array.length; ++i) {
                sb.append(Integer.toHexString((array[i] & 0xFF) | 0x100)
                        .substring(1, 3));
            }
            return sb.toString();
        } catch (java.security.NoSuchAlgorithmException e) {
        }
        return null;
    }

    public static void main(String[] args) throws IOException {
        getBusiness("39.90,116.38");
    }
}
