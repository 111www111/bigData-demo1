package org.wyt.flume.filter;


import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.wyt.flume.utils.ETLUtil;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * flume-head 过滤器
 */
public class HeadFilter implements Interceptor {

    /**
     * 头 head的key
     */
    private static final String headKey = "topic";
    /**
     * kafka 队列
     */
    private static final String kafkaTopicStartKey = "topic_start";
    private static final String kafkaTopicEventKey = "topic_event";
    /**
     * 批处理池
     */
    private List<Event> results = new ArrayList<>();


    /**
     * 初始化
     */
    @Override
    public void initialize() {

    }


    /**
     * 单个事件处理方法 当body中包含wyt时，将type设置为wyt，否则设置为other
     * @param event
     * @return
     */
    @Override
    public Event intercept(Event event) {
        //需要获取到内容也就是事件中的body
        byte[] body = event.getBody();
        //先做一下 utf-8的处理
        String content = new String(body, StandardCharsets.UTF_8);
        //判断字符是否为null
        if(StringUtils.isEmpty(content)){
            return null;
        }
        //标记位
        boolean checkLogFlag = true;
        //包含"en":"start" 就是启动日志  否则 就是事件日志，由此添加头标记
        if(content.contains("\"en\":\"start\"")){
            checkLogFlag = ETLUtil.validStartLog(content);
            //写头文件
            event.getHeaders().put(headKey,kafkaTopicStartKey);
        }else {
            checkLogFlag = ETLUtil.validEventLog(content);
            event.getHeaders().put(headKey,kafkaTopicEventKey);
        }
        //checkLogFlag 如果是f 说明日志不合法
        if(!checkLogFlag){
            return null;
        }
        //到这里 可以通过了
        return event;
    }


    /**
     * 批量事件处理方法
     * @param eventsList
     * @return
     */
    @Override
    public List<Event> intercept(List<Event> eventsList) {
        results.clear();
        for (Event event:eventsList) {
            Event intercept = this.intercept(event);
            if (Objects.nonNull(intercept)){
                results.add(intercept);
            }
        }
        return results;
    }


    /**
     * 关闭资源
     */
    @Override
    public void close() {

    }


    /**
     * 建造者模式-使用flume 请参考原版文档
     */
    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new HeadFilter();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
