package com.manning.fia.utils.datagen;

import java.io.File;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

import org.apache.commons.io.FileUtils;

/**
 * There are 40K Zipcodes. Zipcode is 5 digit and starts with 10000
 * Each Zipcode has 1 store. Store id is same as zipcode
 * For simplicity we will assume customer shops at their own zipcode. Assume between 100 customers per zipcode.
 * Each store sell's 10000 items with id's raning from 1-10000. The prices of each item = item_id*0.01+5
 * A customer will buy between 1 to 10 units of a given item per transaction  
 * @author Sameer
 *
 */
public class PetStoreMasterDataGenerator {
    public static int BASE_ZIPCODE=100000;
    public static void main(String[] args) throws Exception{
        String rootFolder = "src/main/resources/petstore/master";
        List<String> zipcodes = new ArrayList<String>();
        for(int i=0;i<40000;i++){
            zipcodes.add(Integer.toString(BASE_ZIPCODE+i));
        }
        FileUtils.writeLines(new File(rootFolder,"zipcodes.txt"), zipcodes);
        List<String> stores = new ArrayList<String>();
        for(String zipcode:zipcodes){
            stores.add(zipcode+","+zipcode);//One zipcode one store policy
        }        
        FileUtils.writeLines(new File(rootFolder,"storeids.txt"), stores);
        
        //Only focus on 50 zipcodes. 1000 customers each
        FileUtils.writeLines(new File(rootFolder,"customers.txt"), new ArrayList<String>());
        for(int i=0;i<50;i++){
            List<String> customers = new ArrayList<String>();
            for(int j=0;j<1000;j++){
                int customerId = 1000+i*1000+j;
                String customerIdStr = Integer.toString(customerId);
                customers.add(customerIdStr+","+"customer_"+customerIdStr+","+Integer.toString(BASE_ZIPCODE+i));
            }
            FileUtils.writeLines(new File(rootFolder,"customers.txt"), customers,true);
        }
        
        Random rnd = new Random();
        List<String> items = new ArrayList<String>();
        FileUtils.writeLines(new File(rootFolder,"items.txt"), new ArrayList<String>());
        for(int i=0;i<1000;i++){
            double price =rnd.nextInt(500)+ rnd.nextDouble();
            BigDecimal bd = new BigDecimal(price);
            bd = bd.setScale(2, RoundingMode.HALF_UP);
            String itemStr = Integer.toString(i) + ","+ "Item_"+Integer.toString(i) +","+ bd.doubleValue();
            items.add(itemStr);
        }
        FileUtils.writeLines(new File(rootFolder,"items.txt"), items,true);

        
        
        
    }
    
}
