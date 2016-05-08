package com.manning.fia.model.petstore;

public class Store {
    public int storeId;
    public String zipcode;
    public Store(int storeId, String zipcode) {
        super();
        this.storeId = storeId;
        this.zipcode = zipcode;
    }
  
    public Store() {} 

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + storeId;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Store other = (Store) obj;
        if (storeId != other.storeId)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "Store [storeId=" + storeId + ", zipcode=" + zipcode + "]";
    }
    
}
