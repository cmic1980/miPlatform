package com.hinacom.bi.domain;

import java.util.List;

public class Group {
    private String[] fields;
    private List<GroupOperation> operationList;

    public String[] getFields() {
        return fields;
    }

    public void setFields(String[] fields) {
        this.fields = fields;
    }

    public List<GroupOperation> getOperationList() {
        return operationList;
    }

    public void setOperationList(List<GroupOperation> operationList) {
        this.operationList = operationList;
    }
}
