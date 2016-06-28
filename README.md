# 第三版本 设计

## 拓扑设计
OrderMessageSpout(Tmall) && PaymentMessageSpout ==> AmountCalculateBolt(Tmall)
OrderMessageSpout(TaoBao) && PaymentMessageSpout ==> AmountCalculateBolt(TaoBao)
PaymentMessageSpout ==> RatioCalculateBolt


## Spout
1、三个Spout分别消费三个topic的消息，每个Spout可以并行消费，每个Spout线程可以多个线程拉取消息，
2、在Spout的consumeMessage接口中解析消息放到Spout的队列中
3、在nextTuple中对消息进行消费
4、调整PaymentMessage的消费速度和OrderMessage的消费速度，例如2:1

## JoinBolt
1、可以设置并行度
2、使用

## AggregateBolt



## WriteBolt

一个Bolt计算PC和无线平台交易总额在各时刻的比例
两个Bolt分别计算天猫和淘宝每分钟的总交易金额




## TODO
1. 不要在代码中调用setNamesrvAddr接口来进行指定，可以通过配置环境变量NAMESRV_ADDR=x.x.x.x
如果你通过调用setNamesrvAddr的方式进行指定，提交代码前记得删除这一行代码。 

