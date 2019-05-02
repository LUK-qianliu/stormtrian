package com.qianliu;

import com.qianliu.Utils.DataUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class LogPoccessBolt extends BaseRichBolt {

    private OutputCollector collector;


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        try {
            byte[] bytes = input.getBinaryByField("bytes");
            String value = new String(bytes);

            /*
             * 进来的字符串形如：13999999999	116.410588,39.880172	[2019-29-04/29/19 13:29:33]
             * 进行解析
             * */
            String[] splits = value.split("\t");
            String phone = splits[0];
            String[] local = splits[1].split(",");
            String longitude = local[0]; //经度
            String latitude = local[1]; //纬度
            long time = DataUtils.getInstance().getTime(splits[2]); //[2019-29-04/29/19 13:29:33]格式的时间转换成一个时间戳

            //发向下一个bolt
            this.collector.emit(new Values(phone,longitude,latitude,time));
            //System.out.println(phone+"\t"+longitude+"\t"+latitude+"\t"+time);

            this.collector.ack(input);
        } catch (Exception e) {
            this.collector.fail(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("phone", "longitude","latitude","time"));
    }
}
