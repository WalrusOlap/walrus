package org.pcg.walrus.server.controller;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.zookeeper.KeeperException;
import org.pcg.walrus.common.exception.WServerException;
import org.pcg.walrus.server.model.ResponseTemplate;

import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * global exception handler
 */
@ControllerAdvice
public class SharkExceptionController {

    private static final Logger log = LoggerFactory.getLogger(SharkExceptionController.class);

    @ExceptionHandler(value = Exception.class)
    @ResponseBody
    public ResponseTemplate exception(HttpServletRequest req, Exception exception) throws Exception {
        String message = exception == null ? "SYSTEM_ERROR" : ExceptionUtils.getFullStackTrace(exception);
        message = message.length() > 50 ? message.substring(0, 200) + "..." : message;
        log.error(ExceptionUtils.getFullStackTrace(exception));
        return new ResponseTemplate(500, message);
    }

    @ExceptionHandler(value = MethodArgumentTypeMismatchException.class)
    @ResponseBody
    public ResponseTemplate exception(HttpServletRequest req, MethodArgumentTypeMismatchException exception) throws Exception {
        String message = String.format("参数格式错误,日期请遵循[yyyy-MM-dd HH:mm:ss]格式, 异常：%s", exception.getMessage());
        log.error(message);
        return new ResponseTemplate(500, message);
    }

    @ExceptionHandler(value = MissingServletRequestParameterException.class)
    @ResponseBody
    public ResponseTemplate exception(HttpServletRequest req, MissingServletRequestParameterException exception) throws Exception {
        String message = String.format("必要参数缺失：%s", exception.getMessage());
        log.error(message);
        return new ResponseTemplate(500, message);
    }

    @ExceptionHandler(value = WServerException.class)
    @ResponseBody
    public ResponseTemplate exception(HttpServletRequest req, WServerException exception) throws Exception {
        return new ResponseTemplate(exception.getErrorCode(), exception.getMessage());
    }

    @ExceptionHandler(value = KeeperException.class)
    @ResponseBody
    public ResponseTemplate exception(HttpServletRequest req, KeeperException exception) throws Exception {
        String message = String.format("读取meta错误：%s", exception.getMessage());
        return new ResponseTemplate(500, exception.getMessage());
    }
}
