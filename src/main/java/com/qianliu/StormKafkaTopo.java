package com.qianliu;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class StormKafkaTopo {
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        //storm的builder
        TopologyBuilder builder = new TopologyBuilder();

        //创建spout，采集kafka数据
        BrokerHosts hosts = new ZkHosts("192.168.48.138:2181");
        String topic = "logstash";
        String zkRoot = "/"+topic;
        String id = UUID.randomUUID().toString();
        SpoutConfig spoutConfig = new SpoutConfig(hosts,topic,zkRoot,id);
        spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime(); //设置每次读取从上次读取结束的位置开始
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

        //builder装载spout
        String SPOUT_ID = KafkaSpout.class.getSimpleName();//产生一个简称ID
        builder.setSpout(SPOUT_ID,kafkaSpout);

        //builder装载bolt，并且与spout创建关联关系
        String BOLT_ID = LogPoccessBolt.class.getSimpleName();
        builder.setBolt(BOLT_ID,new LogPoccessBolt()).shuffleGrouping(SPOUT_ID);

        //builder装载bolt2，并且与bolt创建关联关系
        String BOLT2_ID = HbaseBoltByUser.class.getSimpleName();
        builder.setBolt(BOLT2_ID,new HbaseBoltByUser()
                //统计20秒内的数据，每5秒计算一次结果
                .withWindow(new BaseWindowedBolt.Duration(20, TimeUnit.SECONDS), new BaseWindowedBolt.Duration(5, TimeUnit.SECONDS)),1)
                .shuffleGrouping(BOLT_ID);

        //提交任务  -----两种模式 本地模式和集群模式
        Config config = new Config();
        config.setNumWorkers(1);
        if (args.length>0) {
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());//
        }else {
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("storm2kafka", config, builder.createTopology());
        }
    }
}
