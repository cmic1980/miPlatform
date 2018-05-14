package com.hinacom.bi.domain;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

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

    public Object getObjectValue(){
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        String s = df.format(new Date());
        try {
            Date date = df.parse(this.value.toString());
            return date;
        } catch (ParseException e) {
            e.printStackTrace();
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
