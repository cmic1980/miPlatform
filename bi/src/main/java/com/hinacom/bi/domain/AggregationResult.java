package com.hinacom.bi.domain;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import com.mongodb.BasicDBObject;
import org.bson.BsonDocument;
import org.bson.Document;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class AggregationResult {
    private List<AggregationResultItem> itemList;

    AggregationResult(AggregationResults<BasicDBObject> aggregationResults){
        List<Document> documentList = (List) aggregationResults.getRawResults().get("result");
        this.itemList = documentList
                .parallelStream()
                .map(doc -> {
                    AggregationResultItem aggregationResultItem = AggregationResultItem.parse(doc);
                    return aggregationResultItem;
                })
                .collect(Collectors.toList());
    }

    public static AggregationResult parse(AggregationResults<BasicDBObject> aggregationResults)
    {
        AggregationResult aggregationResult = new AggregationResult(aggregationResults);
        return aggregationResult;
    }

    public List<AggregationResultItem> getItemList() {
        return itemList;
    }

    public void merge(AggregationResult source)
    {
        HashMap<String,AggregationResultItem> targetMap = new HashMap<>();
        this.itemList.forEach((s)-> {targetMap.put(s.toString(), s);});

        HashMap<String,AggregationResultItem> sourceMap = new HashMap<>();
        source.getItemList().forEach((s)-> {sourceMap.put(s.toString(), s);});
        targetMap.putAll(sourceMap);

        this.itemList = targetMap.values().parallelStream().collect(Collectors.toList());
    }
}
