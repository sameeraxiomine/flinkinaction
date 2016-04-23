package com.manning.fia.utils.datagen;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * There are 40K Zipcodes. Zipcode is 5 digit and starts with 10000
 * Each Zipcode has 1 store. Store id is same as zipcode
 * For simplicity we will assume customer shops at their own zipcode. Assume between 100 customers per zipcode.
 * Each store sell's 10000 items with id's raning from 1-10000. The prices of each item = item_id*0.01+5
 * A customer will buy between 1 to 10 units of a given item per transaction  
 * @author Sameer
 *
 */
public class PetStoreDataGenerator {
    public static void main(String[] args) throws Exception{
        String rootMasterFolder = "src/main/resources/petstore/master";
        String rootTransactionFolder = "src/main/resources/petstore/transaction";
        String rootTransactionsItemsFolder = "src/main/resources/petstore/transactionsitems";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        long dtTime = sdf.parse(args[0]).getTime();
        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHHmmss");
        File dtFolder1=new File(rootTransactionFolder,args[0]);
        if(dtFolder1.exists()){
            FileUtils.deleteQuietly(dtFolder1);
        }        
        dtFolder1.mkdir();
        File dtFolder2=new File(rootTransactionsItemsFolder,args[0]);
        if(dtFolder2.exists()){
            FileUtils.deleteQuietly(dtFolder2);
        }        
        dtFolder2.mkdir();
        
        
        List<String> items = FileUtils.readLines(new File(rootMasterFolder,"items.txt"));
        Map<Integer,Double> priceByItemId = new HashMap<Integer,Double>();
        Map<Integer,String> descByItemId = new HashMap<Integer,String>();
        for(String it:items){
            if(!StringUtils.isBlank(it)){
                String[] its = StringUtils.split(it, ",");
                priceByItemId.put(Integer.parseInt(its[0]), Double.parseDouble(its[2]));
                descByItemId.put(Integer.parseInt(its[0]),its[2]);
            }            
        }
        
        Random rnd = new Random();
        
        List<String> customers = FileUtils.readLines(new File(rootMasterFolder,"customers.txt"));
        long transactionNoBase=dtTime;
        int customerCnt = 0;
        List<String> transactions = new ArrayList<String>();
        List<String> transactionItems = new ArrayList<String>();
        int transactionFileCount=0;
        int totalTransactions =0;
        int totalTransactionItems =0;
        for(String customer:customers){
            if(StringUtils.isBlank(customer)) continue;
            totalTransactions++;
            String[] carr = StringUtils.split(customer, ",");
            int noOfTransactionItems = rnd.nextInt(10)+1;
            long tNo = transactionNoBase++;
            int storeId = Integer.parseInt(carr[2]);
            int custId = Integer.parseInt(carr[0]);
            long time = dtTime+rnd.nextInt(86400)*1000;
            Date tDate = new Date(time);
            transactions.add(custId+","+tNo+","+storeId+","+sdf2.format(tDate));
            totalTransactionItems=totalTransactionItems+noOfTransactionItems;
            for(int j=0;j<noOfTransactionItems;j++){
                int qty = rnd.nextInt(100)+1;           
                int itemId = rnd.nextInt(1000);                
                double price = priceByItemId.get(itemId);
                String itemDesc = descByItemId.get(itemId);
                transactionItems.add(custId+","+tNo+","+itemId+","+itemDesc+","+qty+","+price+","+sdf2.format(tDate));
            }
            if(totalTransactionItems%10000==0){
                FileUtils.writeLines(new File(dtFolder1,"data_"+transactionFileCount), transactions,true);
                FileUtils.writeLines(new File(dtFolder2,"data_"+transactionFileCount), transactionItems,true);
                transactions.clear();
                transactionItems.clear();
                transactionFileCount++;
            }
            
        }
        if(!transactions.isEmpty()){
            FileUtils.writeLines(new File(dtFolder1,"data_"+transactionFileCount), transactions,true);
            FileUtils.writeLines(new File(dtFolder1,"data_"+transactionFileCount), transactionItems,true);
        }
        System.out.println("Total Number of Transactions " + totalTransactions);
        System.out.println("Total Number of Transaction Items " + totalTransactionItems);
        
    }
    
}
