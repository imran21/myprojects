package com.apptium.util;

import org.slf4j.MDC;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class FlowidInteceptor extends HandlerInterceptorAdapter
{
    @Override
    public boolean preHandle(final HttpServletRequest request, final HttpServletResponse 
            response, final Object handler) throws Exception {
        String trackId=request.getHeader("trackId");
        if(trackId!=null)
            MDC.put("trackId", trackId);
        return super.preHandle(request, response, handler);
    }

    @Override
    public void afterCompletion(final HttpServletRequest request, final HttpServletResponse 
            response, final Object handler, final Exception ex) throws Exception {
        MDC.remove("trackId");
        super.afterCompletion(request, response, handler, ex);
    }
}
