package com.hinacom.bi.domain;

import com.alibaba.fastjson.JSONObject;
import org.bson.Document;

import java.util.HashMap;

public class AggregationResultItem extends Document {
    public static AggregationResultItem parse(Document document) {
        AggregationResultItem aggregationResultItem = new AggregationResultItem();

        document.forEach((p, v) -> {
            if (p.equals("_id") == false) {
                aggregationResultItem.put(p, v);
            } else {
                if (document.get("_id") instanceof Document) {
                    var map = (Document) document.get("_id");
                    aggregationResultItem.putAll(map);
                } else {
                    aggregationResultItem.put("_id", document.get("_id"));
                }
            }
        });
        return aggregationResultItem;
    }

}
