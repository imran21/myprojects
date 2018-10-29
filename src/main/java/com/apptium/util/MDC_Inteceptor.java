package com.apptium.util;

import org.slf4j.MDC;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Objects;

public class MDC_Inteceptor extends HandlerInterceptorAdapter
{
    @Override
    public boolean preHandle(final HttpServletRequest request, final HttpServletResponse 
            response, final Object handler) throws Exception {
        MDC.put("accountName", Objects.toString(request.getHeader("ep-accountname"),"NA") );
        MDC.put("appName", Objects.toString( request.getHeader("ep-appname"),"NA"));
        MDC.put("userName",  Objects.toString( request.getParameter("ep-username"),"NA"));
        return super.preHandle(request, response, handler);
    }

    @Override
    public void afterCompletion(final HttpServletRequest request, final HttpServletResponse 
            response, final Object handler, final Exception ex) throws Exception {
        MDC.remove("accountName");
        MDC.remove("appName");
        MDC.remove("userName");
        super.afterCompletion(request, response, handler, ex);
    }
}
