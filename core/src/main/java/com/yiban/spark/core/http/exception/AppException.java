package com.yiban.spark.core.http.exception;

import java.util.List;
import com.yiban.spark.core.http.exception.ErrorResult.Error;
import org.slf4j.helpers.MessageFormatter;

public class AppException extends RuntimeException {
    private String domain;
    private String code;
    private List<Error> errors;

    public AppException(BaseExceptionCode exceptionCode, Throwable cause, Object... msgArgs) {
        super(resolveExceptionMessage(exceptionCode.getTemplateMessage(), msgArgs), cause);
        this.code = exceptionCode.getCode();
    }

    public AppException(BaseExceptionCode exceptionCode, Object... msgArgs) {
        super(resolveExceptionMessage(exceptionCode.getTemplateMessage(), msgArgs));
        this.code = exceptionCode.getCode();
    }

    public AppException(String message, Throwable cause) {
        super(message, cause);
    }

    public AppException(String message) {
        super(message);
    }

    private static String resolveExceptionMessage(String messageTemplate, Object[] msgArgs) {
        return MessageFormatter.arrayFormat(messageTemplate, msgArgs).getMessage();
    }

    public AppException(ErrorResult errorResult) {
        super(errorResult.getMessage());
        this.code = errorResult.getCode();
        this.domain = errorResult.getDomain();
        this.errors = errorResult.getErrors();
    }

    public String getDomain() {
        return this.domain;
    }

    public String getCode() {
        return this.code;
    }

    public List<Error> getErrors() {
        return this.errors;
    }
}
