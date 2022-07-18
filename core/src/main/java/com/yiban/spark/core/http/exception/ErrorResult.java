package com.yiban.spark.core.http.exception;

import java.util.ArrayList;
import java.util.List;

public class ErrorResult {
    private String domain;
    private String code;
    private String message;
    private List<ErrorResult.Error> errors;

    public ErrorResult() {
    }

    public ErrorResult(String code, String message) {
        this.code = code;
        this.message = message;
    }

    public void addError(String name, String message) {
        if (this.errors == null) {
            this.errors = new ArrayList();
        }

        this.errors.add(new ErrorResult.Error(name, message));
    }

    public String getDomain() {
        return this.domain;
    }

    public String getCode() {
        return this.code;
    }

    public String getMessage() {
        return this.message;
    }

    public List<ErrorResult.Error> getErrors() {
        return this.errors;
    }

    public void setDomain(final String domain) {
        this.domain = domain;
    }

    public void setCode(final String code) {
        this.code = code;
    }

    public void setMessage(final String message) {
        this.message = message;
    }

    public void setErrors(final List<ErrorResult.Error> errors) {
        this.errors = errors;
    }

    public static class Error {
        private String name;
        private String message;

        public Error() {
        }

        public Error(String name, String message) {
            this.name = name;
            this.message = message;
        }

        public String getName() {
            return this.name;
        }

        public String getMessage() {
            return this.message;
        }

        public void setName(final String name) {
            this.name = name;
        }

        public void setMessage(final String message) {
            this.message = message;
        }
    }
}
