package com.manning.fia.model.petstore;

public class TransactionItem {
    public int storeId;
    public long transactionId;
    public int itemId;
    public String itemDesc;
    public int itemQty;
    public double itemPrice;
    public double transactionItemValue;
    public long time;
    
    
    public TransactionItem(int storeId, long transactionId, int itemId,
            String itemDesc, int itemQty, double itemPrice, long time) {
        super();
        this.storeId = storeId;
        this.transactionId = transactionId;
        this.itemId = itemId;
        this.itemDesc = itemDesc;
        this.itemQty = itemQty;
        this.itemPrice = itemPrice;
        this.transactionItemValue = this.itemQty * this.itemPrice;
        this.time = time;
    }

    public TransactionItem(){}
    

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + storeId;
        result = prime * result
                + (int) (transactionId ^ (transactionId >>> 32));
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
        TransactionItem other = (TransactionItem) obj;
        if (storeId != other.storeId)
            return false;
        if (transactionId != other.transactionId)
            return false;
        return true;
    }


    @Override
    public String toString() {
        return "TransactionItem [storeId=" + storeId + ", transactionId="
                + transactionId + ", itemId=" + itemId + ", itemDesc="
                + itemDesc + ", itemQty=" + itemQty + ", itemPrice="
                + itemPrice + ", transactionItemValue=" + transactionItemValue
                + ", time=" + time + "]";
    }



}
