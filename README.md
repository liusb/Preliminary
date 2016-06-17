# 实现思路

## 拓扑设计

三个Spout分别消费三个topic的消息

一个Bolt计算PC和无线平台交易总额在各时刻的比例
两个Bolt分别计算天猫和淘宝每分钟的总交易金额

OrderMessageSpout(Tmall) && PaymentMessageSpout ==> AmountCalculateBolt(Tmall)
OrderMessageSpout(TaoBao) && PaymentMessageSpout ==> AmountCalculateBolt(TaoBao)
PaymentMessageSpout ==> RatioCalculateBolt

## Spout设计


## Bolt设计

### AmountCalculateBolt


## TODO
1. 不要在代码中调用setNamesrvAddr接口来进行指定，可以通过配置环境变量NAMESRV_ADDR=x.x.x.x
如果你通过调用setNamesrvAddr的方式进行指定，提交代码前记得删除这一行代码。 

