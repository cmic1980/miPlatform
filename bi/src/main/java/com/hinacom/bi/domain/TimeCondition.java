package com.hinacom.bi.domain;

import java.time.LocalDate;
import java.util.Date;

public class TimeCondition {
    private String field;
    private Date start;
    private Date end;

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }


    public Date getStart() {
        return start;
    }

    public void setStart(Date start) {
        this.start = start;
    }

    public Date getEnd() {
        return end;
    }

    public void setEnd(Date end) {
        this.end = end;
    }
}
