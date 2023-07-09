package com.wyt.flume.utils;

import org.apache.commons.lang.math.NumberUtils;

/**
 * 校验数据工具类
 */
public class ETLUtil {


    /**
     * 校验启动日志
     * @param content
     * @return
     */
    public static boolean validStartLog(String content){
        String trimStr = content.trim();
        if(trimStr.startsWith("{")  && trimStr.endsWith("}") ){
            return true;
        }else {
            return false;
        }
    }

    /**
     * 校验事件日志
     * @param content
     * @return
     */
    public static boolean validEventLog(String content){
        String trimStr = content.trim();

        String[] str = trimStr.split("\\|");
        if(str.length != 2){
            return false;
        }

        //判断时间戳 长度  纯数字
        if(str[0].length() != 13 || !NumberUtils.isDigits(str[0])){
            return false;
        }

        if(str[1].startsWith("{") && str[1].endsWith("}")){
            return true;
        }

        return false;
    }

}
