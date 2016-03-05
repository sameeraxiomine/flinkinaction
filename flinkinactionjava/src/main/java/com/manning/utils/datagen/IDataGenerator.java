package com.manning.utils.datagen;

import java.util.List;

public interface IDataGenerator<T> {
    public void setData(List<T> data);
    public List<T> getData();
    public void generateData();    
}
