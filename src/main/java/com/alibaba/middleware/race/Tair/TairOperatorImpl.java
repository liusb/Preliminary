package com.alibaba.middleware.race.Tair;

import com.alibaba.middleware.race.RaceConfig;
import com.taobao.tair.DataEntry;
import com.taobao.tair.Result;
import com.taobao.tair.ResultCode;
import com.taobao.tair.impl.DefaultTairManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


/**
 * 读写tair所需要的集群信息，如masterConfigServer/slaveConfigServer地址/
 * group 、namespace我们都会在正式提交代码前告知选手
 */
public class TairOperatorImpl {
    private static Logger LOG = LoggerFactory.getLogger(TairOperatorImpl.class);

    private DefaultTairManager _tairManager;
    private int _namespace;

    public TairOperatorImpl(String masterConfigServer,
                            String slaveConfigServer,
                            String groupName,
                            int namespace) {
        List<String> confServers = new ArrayList<String>();
        confServers.add(masterConfigServer);
        if (slaveConfigServer != null) {
            confServers.add(slaveConfigServer);
        }
        _tairManager = new DefaultTairManager();
        _tairManager.setConfigServerList(confServers);
        _tairManager.setGroupName(groupName);
        _tairManager.init();
        _namespace = namespace;
    }

    public TairOperatorImpl() {
        this(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
                RaceConfig.TairGroup, RaceConfig.TairNamespace);
    }

    public boolean write(Serializable key, Serializable value) {
        ResultCode rc = _tairManager.put(_namespace, key, value);
        if (rc.isSuccess()) {
            //LOG.info("%%%%%%: tair write:(" + key +", " + value + ") put success.");
            return true;
        } else if (ResultCode.VERERROR.equals(rc)) {
            // 版本错误的处理代码
            LOG.error("%%%%%%: tair write error: " + rc.getCode() +", " + rc.getMessage());
        } else {
            // 其他失败的处理代码
            LOG.error("%%%%%%: tair write error: " + rc.getCode() +", " + rc.getMessage());
        }
        return false;
    }

    public Object read(Serializable key) {
        Result<DataEntry> result = _tairManager.get(_namespace, key);
        if (result.isSuccess()) {
            DataEntry entry = result.getValue();
            if(entry != null) {
                LOG.info("%%%%%%: tair read success:(" + key + ", " + entry.getValue() + ").");
                return entry.getValue();
            } else {
                // 数据不存在
                LOG.error("%%%%%%: tair read error: key " + key + " not exists.");
            }
        } else {
            // 异常处理
            LOG.error("%%%%%%: tair read error: ", result.getRc().getCode() + ", " + result.getRc().getMessage());
        }
        return null;
    }

    public boolean remove(Serializable key) {
        ResultCode rc = _tairManager.delete(_namespace, key);
        if (rc.isSuccess()) {
            LOG.info("%%%%%%: tair remove success: key " + key);
            return true;
        } else {
            LOG.error("%%%%%%: tair remove error: " + rc.getCode() +", " + rc.getMessage());
        }
        return false;
    }

    public void close(){
        _tairManager.close();
    }

    // 天猫的分钟交易额写入tair
    public static void testTair(String [] args) throws Exception {
        TairOperatorImpl tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer,
                RaceConfig.TairSalveConfigServer, RaceConfig.TairGroup, RaceConfig.TairNamespace);
        //假设这是付款时间
        Long millisTime = System.currentTimeMillis();
        //由于整分时间戳是10位数，所以需要转换成整分时间戳
        Long minuteTime = (millisTime / 1000 / 60) * 60;
        //假设这一分钟的交易额是100;
        Double money = 100.0;
        //写入tair
        tairOperator.write(RaceConfig.prex_tmall + minuteTime, money);
        System.out.println("write over!!!!!!!!!");

        tairOperator.read(RaceConfig.prex_tmall + minuteTime);
        System.out.println("read over!!!!!!!!!");

        tairOperator.remove(RaceConfig.prex_tmall + minuteTime);
        System.out.println("remove over!!!!!!!!!");

        tairOperator.close();
        System.out.println("tair client over!!!!!!!!!");

    }
}
