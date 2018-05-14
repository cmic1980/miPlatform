package com.hinacom.bi.service.impl;

import com.hinacom.bi.service.DefaultService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Set;

@Service
public class DefaultServiceImpl implements DefaultService {
    @Autowired
    private MongoTemplate template;

    @Override
    public Set<String> getCollectionNames() {
        var collectionNames = template.getCollectionNames();
        return collectionNames;
    }
}
