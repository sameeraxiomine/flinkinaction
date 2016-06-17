package com.manning.fia.transformations.media;

import com.manning.fia.model.media.PageInformation;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * Created by hari on 6/16/16.
 */
public class DomainObjectBasedPageInformationParser implements
        MapFunction<String, PageInformation> {
    @Override
    public PageInformation map(String value) throws Exception {
        PageInformation pageInformation = PageInformationParser.mapRow(value);
        return pageInformation;
    }
}
