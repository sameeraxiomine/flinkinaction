package com.manning.fia.transformations.media;

import com.manning.fia.model.media.PageInformation;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;


@SuppressWarnings("serial")
public class PageInformationParser {


    public static List<String> parseData() throws Exception {
        final Scanner scanner = new Scanner(ClassLoader.class.getResourceAsStream("/media/pipe/pageinformation"));
        List<String> PageInformations = new ArrayList<>(0);
        while (scanner.hasNext()) {
            String value = scanner.nextLine();
            PageInformations.add(value);
        }
        return PageInformations;
    }


    public static PageInformation mapRow(String value) {

        final String[] tokens = value.toLowerCase().split("\\|");

        final long pageId = Long.valueOf(tokens[0]);

        final String author = tokens[1];
        final String url = tokens[2];
        final String description = tokens[3];
        final String contentType = tokens[4];
        final long publishDate = Long.valueOf(tokens[5]);


        final String section = tokens[6];
        final String subSection = tokens[7];
        final String topic = tokens[8];
        final PageInformation pageInformation = new PageInformation(pageId, author, url, description, contentType,
                publishDate, section, subSection, topic);
        return pageInformation;

    }


}
