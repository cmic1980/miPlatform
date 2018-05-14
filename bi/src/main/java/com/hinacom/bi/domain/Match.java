package com.hinacom.bi.domain;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Pattern;

public class Match {
    private String field;
    private MatchOperate operate;
    private Object value;
    private MatchLinkOperate link;

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public MatchOperate getOperate() {
        return operate;
    }

    public void setOperate(MatchOperate operate) {
        this.operate = operate;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public Object getObjectValue() throws ParseException {
        boolean isMatch = Pattern.matches("^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}[Z]*$", this.getValue().toString());
        if(isMatch)
        {
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
            Date date = df.parse(this.value.toString());
            return date;
        }
        return this.value;
    }

    public MatchLinkOperate getLink() {
        return link;
    }

    public void setLink(MatchLinkOperate link) {
        this.link = link;
    }
}
