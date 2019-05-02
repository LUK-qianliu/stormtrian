package com.qianliu;

import com.qianliu.Utils.Config;
import com.qianliu.Utils.HBaseUtils;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;

import java.util.HashMap;
import java.util.Map;

/*
* 利用滑动窗口，每5秒中一个窗口，1秒更新一次数据
* */
public class HbaseBoltByUser extends BaseWindowedBolt {

    OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(TupleWindow inputWindow) {
        HashMap<String,Integer> hashMap = new HashMap<String,Integer>(); //hashmap存放一个窗口内的数据<local,times>,local是经纬度，times是一个窗口内该local的访问次数

        try {
            for (Tuple input : inputWindow.get()){
                //删一个bolt发过来的信息
                String phone = input.getStringByField("phone");
                String longitude = input.getStringByField("longitude");
                String latitude = input.getStringByField("latitude");
                long time = input.getLongByField("time");

                //System.out.println(phone+"\t"+longitude+"\t"+latitude+"\t"+time);

                //如果第一次放入，则<local,0>
                //如果不是第一次放入，则<local,times++>
                if(hashMap.get(longitude+","+latitude)==null){
                    hashMap.put(longitude+","+latitude,0);
                }else {
                    int times = (int)hashMap.get(longitude+","+latitude); //统计次数
                    hashMap.put(longitude+","+latitude,++times);
                }

                collector.ack(input);
            }

            //将hashmap中的数据放入hbase
            HTable table = new HTable(HBaseUtils.con.getConfiguration(), Config.HBaseTableName);//连接hbase
            for (Map.Entry<String,Integer> entry : hashMap.entrySet()) {
                Put put = new Put(Bytes.toBytes(entry.getKey()));// rowkey是local
                //添加数据
                put.addColumn(Config.HBaseColumn1.getBytes(), Config.col1_info1.getBytes(), (entry.getValue()+"").getBytes());//访问次数
                table.put(put);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
