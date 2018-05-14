package com.hinacom.bi.service.impl;

import com.hinacom.bi.domain.*;
import com.hinacom.bi.service.AggregationService;
import com.mongodb.BasicDBObject;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.MatchOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.stereotype.Service;


import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.springframework.data.mongodb.core.aggregation.Aggregation.group;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.match;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.newAggregation;

@Service
public class AggregationServiceImpl implements AggregationService {
    @Autowired
    private MongoTemplate template;

    @Override
    public AggregationResult aggregate(AggregationParameter aggregationParameter) {
        List<AggregationOperation> aggregationOperationList = new ArrayList<AggregationOperation>();

        // matchs
        List<Match> matchList = aggregationParameter.getMatchList();
        Criteria criteria = this.buildCriteria(matchList);
        if (criteria != null) {
            var match = match(criteria);
            aggregationOperationList.add(match);
        }

        // group
        Group groupParameter = aggregationParameter.getGroup();
        String[] groupFields = groupParameter.getFields();
        // group - fields
        var group = group(groupFields);
        // group - operations
        List<GroupOperation> operationList = groupParameter.getOperationList();
        for (GroupOperation operation : operationList) {
            String alias = "";
            if (StringUtils.isBlank(operation.getAlias())) {
                alias = operation.getField();
            } else {
                alias = operation.getAlias();
            }

            switch (operation.getType()) {
                case count:
                    group = group.count().as(alias);
                    break;
                case sum:
                    group = group.sum(operation.getField()).as(alias);
                    break;
                case avg:
                    group = group.avg(operation.getField()).as(alias);
                    break;
                case min:
                    group = group.min(operation.getField()).as(alias);
                    break;
                case max:
                    group = group.max(operation.getField()).as(alias);
                    break;
            }
        }
        aggregationOperationList.add(group);

        AggregationOperation[] aggregationOperationArray = new AggregationOperation[aggregationOperationList.size()];
        aggregationOperationList.toArray(aggregationOperationArray);

        Aggregation agg = newAggregation(aggregationOperationArray);
        AggregationResults<BasicDBObject> aggregationResults = template.aggregate(agg, "StatisticOrder", BasicDBObject.class);
        AggregationResult aggregationResult = AggregationResult.parse(aggregationResults);
        return aggregationResult;
    }

    private Criteria buildCriteria(List<Match> matchList) {
        Criteria criteria = null;
        MatchLinkOperate linkOperate = null;
        for (Match match : matchList) {
            String field = match.getField();
            Criteria iCriteria = org.springframework.data.mongodb.core.query.Criteria.where(field);

            switch (match.getOperate()) {
                case lt:
                    iCriteria.lt(match.getObjectValue());
                    break;
                case lte:
                    iCriteria.lte(match.getObjectValue());
                    break;
                case gt:
                    iCriteria.gt(match.getObjectValue());
                    break;
                case gte:
                    iCriteria.gte(match.getObjectValue());
                    break;
                case in:
                    iCriteria.in(match.getObjectValue());
                    break;
            }

            if (linkOperate == null) {
                criteria = iCriteria;
            } else {
                switch (linkOperate) {
                    case and:
                        criteria = criteria.andOperator(iCriteria);
                        break;
                    case or:
                        criteria = criteria.orOperator(iCriteria);
                        break;
                }
            }

            if (match.getLink() != null) {
                linkOperate = match.getLink();
            }
        }
        return criteria;
    }
}
