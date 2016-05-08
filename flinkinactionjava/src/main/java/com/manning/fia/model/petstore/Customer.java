package com.manning.fia.model.petstore;

public class Customer {
    public int customerId;
    public String customerName;
    public String zipcode;
    public Customer(int custmerId, String customerName, String zipcode) {
        super();
        this.customerId = custmerId;
        this.customerName = customerName;
        this.zipcode = zipcode;
    }
    
    public Customer(){}
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + customerId;
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
        Customer other = (Customer) obj;
        if (customerId != other.customerId)
            return false;
        return true;
    }
    @Override
    public String toString() {
        return "Customer [customerId=" + customerId + ", customerName="
                + customerName + ", zipcode=" + zipcode + "]";
    }
    
    
}
