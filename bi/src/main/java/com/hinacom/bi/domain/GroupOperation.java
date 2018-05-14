package com.hinacom.bi.domain;

public class GroupOperation {
    private GroupOperationType type;
    private String field;
    private String alias;

    public GroupOperationType getType() {
        return type;
    }

    public void setType(GroupOperationType type) {
        this.type = type;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }
}
