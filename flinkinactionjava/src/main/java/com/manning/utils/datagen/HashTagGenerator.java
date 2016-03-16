package com.manning.utils.datagen;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.apache.flink.shaded.com.google.common.base.Throwables;

/**
 * This class generates data for word count application
 * 
 */
public class HashTagGenerator implements IDataGenerator<String>{
    public static int NO_OF_HOURS_IN_DAY = 24;
    public static int NO_OF_MINS_IN_HOUR = 60;
    private Random randomNumberGenerator = new Random();
    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
    private Date inputDate;
    private String[] hashtags = { "#Flink", "#Flink", "#Flink", "#Flink", "#Flink",
                                  "#Flink", "#ChicagoFlinkMeetup", "#ChicagoFlinkMeetup",
                                  "#DCFlinkMeetup", "#NYCFlinkMeetup", "#ApacheBeam",
                                  "#ApacheBeam", "#ApacheBeam", "#GoogleDataFlow",
                                  "#GoogleDataFlow" };

    private List<String> data;
    public HashTagGenerator(String defaultDt, Long randomSeed){
        try{
            SimpleDateFormat inputSDF = new SimpleDateFormat("yyyyMMdd");
            this.inputDate = inputSDF.parse(defaultDt);
            if(randomSeed!=null){
                this.randomNumberGenerator = new Random(randomSeed);    
            }            
        }catch(Exception ex){
            Throwables.propagate(ex);
        }        
    }
    public HashTagGenerator(String defaultDt){
        this(defaultDt, null);
    }
    public HashTagGenerator(){

    }

    public String[] getHashtags() {
        return hashtags;
    }

    public void setHashtags(String[] hashtags) {
        this.hashtags = hashtags;
    }
    
    @Override
    public void setData(List<String> data) {
        this.data = data;
    }
    /**
     * This class generates word count data for batch implementation of word
     * count return value will have multiple lines each with the following
     * format $TIME,$HASHTAG where $TIME is in the format yyyyMMddHHmm Example
     * line is 201603051315,#DCFlinkMeetup
     * 
     */
    @Override
    public void generateData() {
        data = new ArrayList<>();
        Calendar cal = Calendar.getInstance();
        try {
            String[] allHashTags = HashTagGenerator.getSampleHashTags();
            int noOfHashTags = allHashTags.length;
            for (int i = 0; i < NO_OF_HOURS_IN_DAY; i++) {
                for (int j = 0; j < NO_OF_MINS_IN_HOUR; j++) {//Min 0 to 59
                    int index = this.randomNumberGenerator.nextInt(noOfHashTags);
                    String hashTagForMinuteOfDay = allHashTags[index];
                    cal.setTime(inputDate);
                    cal.set(Calendar.HOUR_OF_DAY, i);
                    cal.set(Calendar.MINUTE, j);
                    cal.set(Calendar.SECOND, 0);
                    cal.set(Calendar.MILLISECOND, 0);
                    Date newDate = cal.getTime();
                    String dtTime = sdf.format(newDate);
                    data.add(dtTime + "," + hashTagForMinuteOfDay);
                }
            }            

        } catch (Exception ex) {
            Throwables.propagate(ex);
        }
    }
    
    @Override
    public void generateStreamingData(int hour,int min,int noOfMins) {        
        data = new ArrayList<>();
        Calendar cal = Calendar.getInstance();
        try {
            String[] allHashTags = HashTagGenerator.getSampleHashTags();
            int noOfHashTags = allHashTags.length;
            for(int i = 0;i<noOfMins;i++){
                for (int j = 0; j < 59; j++) {                    
                    int index = this.randomNumberGenerator.nextInt(noOfHashTags);
                    String hashTagForMinuteOfDay = allHashTags[index];
                    cal.setTime(inputDate);
                    cal.set(Calendar.HOUR_OF_DAY, hour);
                    cal.set(Calendar.MINUTE, min);
                    cal.set(Calendar.SECOND, j);
                    cal.set(Calendar.MILLISECOND, 0);
                    Date newDate = cal.getTime();
                    String dtTime = sdf.format(newDate);
                    data.add(dtTime + "," + hashTagForMinuteOfDay);                
                }            
                if(min==59){
                    min=0;
                    if(hour==23){
                        hour=0;
                    }else{
                        hour = hour + 1;
                    }
                }else{
                    min=min+1;
                }
            }

        } catch (Exception ex) {
            Throwables.propagate(ex);
        }
    }
    @Override
    public List<String> getData() {
        return this.data;
    }

    public static String[] getSampleHashTags() {
        return new String[]{ "#Flink", "#Berlin", "#Flink", "#Flink", "#Flink",
                "#Flink", "#ChicagoFlinkMeetup", "#ChicagoFlinkMeetup",
                "#DCFlinkMeetup", "#NYCFlinkMeetup", "#ApacheBeam",
                "#ApacheBeam", "#ApacheBeam", "#GoogleDataFlow",
                "#GoogleDataFlow", "#TUBerlin" };
    }

    public static void main(String[] args) throws Exception {
        HashTagGenerator tagGenerator = new HashTagGenerator("20160316", 1000L);
        tagGenerator.generateData();
        FileUtils.writeLines(new File("src/main/resources/sample/hashtags.txt"), tagGenerator.getData());
        tagGenerator.generateStreamingData(12,0,1);
        FileUtils.writeLines(new File("src/main/resources/sample/streaminghashtags.txt"), tagGenerator.getData());
    }


}
