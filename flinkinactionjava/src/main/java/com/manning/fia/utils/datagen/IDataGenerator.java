package com.manning.fia.utils.datagen;

import java.util.List;

public interface IDataGenerator<T> {
    void setData(List<T> data);
    List<T> getData();
    void generateData();    
    void generateStreamingData(int hour, int min, int noOfMins);
}
