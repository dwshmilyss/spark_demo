package com.yiban.spark.core.http.exception;

public enum ExceptionCode implements BaseExceptionCode {
    S_00009("S00009", "HTTP error, detail [{}]");

    private String code;
    private String templateMessage;

    ExceptionCode(String code, String templateMessage) {
        this.code = code;
        this.templateMessage = templateMessage;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getTemplateMessage() {
        return templateMessage;
    }
}
