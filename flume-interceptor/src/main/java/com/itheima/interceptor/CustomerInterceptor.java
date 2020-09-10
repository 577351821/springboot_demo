package com.itheima.interceptor;

import org.apache.commons.compress.utils.Charsets;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @description:
 * @author: huang
 * @create: 2020-08-21 15:36
 */
public class CustomerInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        String eventBody = new String(event.getBody(), Charsets.UTF_8);
        String[] fields = eventBody.split(" ");
        String time_local = "";
        if (fields.length>11){
            time_local = fields[4];
        }
        Map<String, String> headers = event.getHeaders();
        if(StringUtils.isNotBlank(time_local)){
            headers.put("event_time",time_local);
        }else{
            headers.put("event_time","unknown");
        }

        event.setHeaders(headers);
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {

        List<Event> out = new ArrayList<>();
        for (Event event : events) {
            Event outEvent = intercept(event);
            if(outEvent != null){
                out.add(outEvent);
            }
        }
        return out;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        //返回一个自定义拦截对象即可
        @Override
        public Interceptor build() {
            return new CustomerInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
