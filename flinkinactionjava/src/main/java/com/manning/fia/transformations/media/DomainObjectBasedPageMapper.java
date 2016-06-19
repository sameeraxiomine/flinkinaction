package com.manning.fia.transformations.media;

import com.manning.fia.model.media.Page;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * Created by hari on 6/16/16.
 */
public class DomainObjectBasedPageMapper implements
        MapFunction<String, Page> {
    @Override
    public Page map(String value) throws Exception {
        Page pageInformation = PageParser.mapRow(value);
        return pageInformation;
    }
}
