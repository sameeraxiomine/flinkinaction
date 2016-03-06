package com.manning.utils.datagen;

import java.util.List;

public interface IDataGenerator<T> {
    void setData(List<T> data);
    List<T> getData();
    void generateData();    
}
