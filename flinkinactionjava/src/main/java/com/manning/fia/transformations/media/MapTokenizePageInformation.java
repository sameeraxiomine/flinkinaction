package com.manning.fia.transformations.media;

import com.manning.fia.model.media.PageInformation;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

@SuppressWarnings("serial")
public class MapTokenizePageInformation implements MapFunction<String, Tuple3<String, String, Long>> {
    @Override
    public Tuple3<String, String, Long> map(String value) throws Exception {

        final PageInformation pageInformation=PageInformationParser.mapRow(value);
        final Tuple3<String, String, Long> tuple3 = new Tuple3<>(pageInformation.getUrl(), pageInformation
                .getContentType(), pageInformation.getId());
        return tuple3;
    }
}
