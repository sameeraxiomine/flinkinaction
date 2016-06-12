package com.manning.fia.transformations.media;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.manning.fia.model.media.ApplicationUser;
import com.manning.fia.model.media.PageInformation;
import com.manning.fia.model.media.PageInformation;
import sun.jvm.hotspot.debugger.Page;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;


@SuppressWarnings("serial")
public class PageInformationParser {


    public static List<PageInformation> parse(boolean isJSON) throws Exception {
        if (isJSON) {
            return parseJSON();
        }
        return parseCSV();
    }

    private static List<PageInformation> parseCSV() throws Exception {
        final Scanner scanner = new Scanner(ClassLoader.class.getResourceAsStream("/media/csv/PageInformation.csv"));
        List<PageInformation> PageInformations = new ArrayList<>(0);
        while (scanner.hasNext()) {
            String value = scanner.nextLine();
            PageInformations.add(mapCSVRow(value));
        }
        return PageInformations;
    }


    public static PageInformation mapCSVRow(String value) {

        final String[] tokens = value.toLowerCase().split(",");

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

    public static List<PageInformation> parseJSON() throws Exception {
        final ObjectMapper objectMapper = new ObjectMapper();
        final PageInformation[] pageInformations = objectMapper.readValue(ClassLoader.class.getResourceAsStream
                ("/media/json/pageinformation.json"), PageInformation[].class);
        return Arrays.asList(pageInformations);
    }
}
