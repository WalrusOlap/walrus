package org.pcg.walrus.server.controller;

import org.pcg.walrus.server.model.ResponseTemplate;

import org.springframework.boot.web.servlet.error.ErrorController;

import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestMapping;

import javax.servlet.http.HttpServletRequest;

/**
 * global error handler
 */
@RestController
@RequestMapping(value="${error.path:/error}")
public class SharkErrorController implements ErrorController {

    @RequestMapping(produces = "application/json;charset=UTF-8")
    public ResponseTemplate error(HttpServletRequest request) {
        Integer statusCode = (Integer) request.getAttribute("javax.servlet.error.status_code");
        Exception exception = (Exception) request.getAttribute("javax.servlet.error.exception");
        String message = exception == null ? "SYSTEM_ERROR" : exception.getMessage();
        return new ResponseTemplate(statusCode, message);
    }

    @Override
    public String getErrorPath() {
        return "/error";
    }

}
