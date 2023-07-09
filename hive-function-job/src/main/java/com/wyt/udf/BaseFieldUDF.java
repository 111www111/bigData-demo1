package com.wyt.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 * 自定义UDF
 */
public class BaseFieldUDF extends UDF {

    /**
     *
     * @param line
     * @param key
     * @return UDF 注意 必须叫evaluate
     * @throws JSONException
     */
    public String evaluate(String line, String key) throws JSONException {
        String[] str = line.split("\\|");
        if(str.length != 2){
            return "";
        }

        JSONObject object = new JSONObject(str[1].trim());
        String result = "";

        if("et".equals(key)){
            if(object.has(key)){
                result = object.getString(key);
            }
        }else if("st".equals(key)){
            result = str[0].trim();
        }else {
            JSONObject cm = object.getJSONObject("cm");
            if(cm.has(key)){
                result = cm.getString(key);
            }
        }
        return result;
    }

}
