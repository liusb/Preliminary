package com.alibaba.middleware.race;

import com.alibaba.middleware.race.Tair.TairOperatorImpl;

public class RunApp {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("parameters: tair, producer, consumer, jstorm");
        }else if(args[0].equals("tair")) {
            System.out.println("run tair test");
            TairOperatorImpl.testTair(args);
        }else if(args[0].equals("producer")) {
            System.out.println("run rocketmq producer test");

        }else if(args[0].equals("consumer")) {
            System.out.println("run rocketmq consumer test");

        }else {
            System.out.println("run jstorm test");

        }
    }
}
