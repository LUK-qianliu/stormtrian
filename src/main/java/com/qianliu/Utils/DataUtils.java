package com.qianliu.Utils;

import org.apache.logging.log4j.core.util.datetime.FastDateFormat;
/*1.时间格式化的工具类
* */
public class DataUtils {

    private DataUtils(){}

    private static DataUtils instance;

    public static DataUtils getInstance(){
        if(instance == null){
            instance = new DataUtils();
        }

        return instance;
    }

    //将"yyyy-MM-dd HH:mm:ss"格式的字符串格式化为1556522469000这样的时间戳
    FastDateFormat format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
    public long getTime(String time) throws Exception{
        return format.parse(time.substring(1,time.length()-1)).getTime();
    }
}
