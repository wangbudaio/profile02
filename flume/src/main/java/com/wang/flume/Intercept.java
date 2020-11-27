package com.wang.flume;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;
import java.util.Map;

/**
 * @author 王继昌
 * @create 2020-09-24 19:23
 */
public class Intercept implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();
        byte[] body = event.getBody();
        String s = new String(body);
        if (s.contains("wang")) {
            headers.put("name","wang");
        }else{
            headers.put("name","other");
        }
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        for (Event event : list) {
            intercept(event);
        }
        return list;
    }

    @Override
    public void close() {

    }
    public  static class   mybuilder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new Intercept();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
