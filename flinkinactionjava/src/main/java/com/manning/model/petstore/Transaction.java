package com.manning.model.petstore;

public class Transaction {
    public int storeId;
    public int transactionId;
    public int customerId;
    public long time;
    public Transaction(int storeId, int transactionId, int customerId, long time) {
        super();
        this.storeId = storeId;
        this.transactionId = transactionId;
        this.customerId = customerId;
        this.time = time;
    }
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + storeId;
        result = prime * result + transactionId;
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
        Transaction other = (Transaction) obj;
        if (storeId != other.storeId)
            return false;
        if (transactionId != other.transactionId)
            return false;
        return true;
    }
    @Override
    public String toString() {
        return "Transaction [storeId=" + storeId + ", transactionId="
                + transactionId + ", customerId=" + customerId + ", time="
                + time + "]";
    }
    
    
}
