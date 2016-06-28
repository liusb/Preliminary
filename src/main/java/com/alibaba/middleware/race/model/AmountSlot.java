package com.alibaba.middleware.race.model;

public class AmountSlot {

    public double tmAmount;
    public double tbAmount;
    public double pcAmount;
    public double wirelessAmount;

    @Override
    public String toString() {
        return ", tmAmount:" + tmAmount + ", tbAmount:" + tbAmount + ", wireless/pc:" + wirelessAmount/pcAmount;
    }
}
