package com.alibaba.middleware.race.model;

public class AmountSlot {

    public double tmAmount;
    public double tbAmount;
    public double pcAmount;
    public double wirelessAmount;

    @Override
    public String toString() {
        return ", tmAmount:" + round(tmAmount) + ", tbAmount:" + round(tbAmount)
                + ", wireless/pc:" + round(wirelessAmount)/round(pcAmount);
    }

    public static double round(double value) {
        long tmp = Math.round(value * 100);
        return (double) tmp / 100;
    }
}
