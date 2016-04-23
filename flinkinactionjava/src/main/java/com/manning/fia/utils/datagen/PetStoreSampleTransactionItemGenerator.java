package com.manning.fia.utils.datagen;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;

public class PetStoreSampleTransactionItemGenerator {
    public static void main(String[] args) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        Date tDt = sdf.parse("20151231130000");
        DateTime tDtTime = new DateTime(tDt);
        DateTime tDtTime2 = tDtTime.plusHours(2);
        String[] transactionItemLines = {
                "1000,1,1,1_item,5,1.0," + tDtTime.toDate().getTime(),
                "1000,1,2,2_item,10,100.0," + tDtTime.toDate().getTime(),
                "1000,1,3,3_item,3,200.0," + tDtTime.toDate().getTime(),
                "1001,2,1,1_item,4,1.0," + tDtTime2.toDate().getTime(),
                "1001,2,2,2_item,11,100.0," + tDtTime2.toDate().getTime(),
                "1001,2,3,3_item,7,200.0," + tDtTime2.toDate().getTime(),
                "1002,3,1,1_item,4,1.0," + tDtTime2.toDate().getTime(),
                "1002,3,2,2_item,11,100.0," + tDtTime2.toDate().getTime(),
                "1002,3,3,3_item,7,200.0," + tDtTime2.toDate().getTime(),
                "1003,4,1,1_item,4,1.0," + tDtTime2.toDate().getTime(),
                "1003,4,2,2_item,11,100.0," + tDtTime2.toDate().getTime(),
                "1003,4,3,3_item,7,200.0," + tDtTime2.toDate().getTime(),
                "1004,5,1,1_item,4,1.0," + tDtTime2.toDate().getTime(),
                "1004,5,2,2_item,11,100.0," + tDtTime2.toDate().getTime(),
                "1004,5,3,3_item,7,200.0," + tDtTime2.toDate().getTime(),
                "1005,6,1,1_item,4,1.0," + tDtTime2.toDate().getTime(),
                "1005,6,2,2_item,11,100.0," + tDtTime2.toDate().getTime(),
                "1005,6,3,3_item,7,200.0," + tDtTime2.toDate().getTime(),
                "1006,7,1,1_item,4,1.0," + tDtTime2.toDate().getTime(),
                "1006,7,2,2_item,11,100.0," + tDtTime2.toDate().getTime(),
                "1006,7,3,3_item,7,200.0," + tDtTime2.toDate().getTime() };
        FileUtils
                .writeLines(new File(
                        "./src/main/resources/petstore/sample/input",
                        "transactionitem"), Arrays.asList(transactionItemLines));
    }

}
