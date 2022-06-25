package com.lyc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @ProjectName FlexibleCleaningAndFusion1.0
 * @ClassName SetHbase
 * @Description TODO Hbase配置类
 * @Author lyc
 * @Date 2021/7/9 20:51
 * @Version 1.0
 **/
public class SetHbase {

    /**
     * 配置文件路径
     **/
    public static String conffilepath;

    /**
     * hbase元数据操作对象
     **/
    public static Admin admin;

    /**
     * hbase连接对象
     **/
    public static Connection connection;

    /**
     * @Description: TODO hbase初始化配置
     * @Author: lyc
     * @Date: 2021/7/9 20:58
     * @param ip: zookeeper服务节点ip
     * @param port: zookeeper客户端端口号，默认2181
     * @return: void
     **/
    public void initHbase(String ip, String port) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum",ip);
        configuration.set("hbase.zookeeper.property.clientPort",port);
        connection = ConnectionFactory.createConnection(configuration);
        admin = connection.getAdmin();
    }

    /**
     * @Description: TODO 将解析xml后的数据导入hbase
     * @Author: lyc
     * @Date: 2021/7/9 21:01
     * @param tableName: 导入hbase表名
     * @param item:  存放解析后xml数据集合
     * @return: boolean
     **/
    public boolean fileToHbase(String tableName, Item item) throws IOException {
        if (item.itemmap.isEmpty()){
            return false;
        }
        //获取hbase表中的列名集合
        ArrayList<String> hbaseindex = ReadConfigFile.ReadConfigItem(conffilepath,tableName+"列信息=");
        //获取hbase的rowkey
        String rowkeyindex = ReadConfigFile.ReadConfigItem(conffilepath,tableName+"的RowKey=").get(0);
        //创建hbase的表对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        String key = (String)item.itemmap.get(rowkeyindex);
        if (key == null || key.equals("")) {
            return  false;
        }
        //添加数据
        List<Put> puts = new ArrayList<Put>();
        for (String hb:hbaseindex){
            Put put = new Put(key.getBytes());
            System.out.println(hb +" ==== "+ item.itemmap.get(hb));
            if ( item.itemmap.get(hb) == null){
                put.addColumn(Bytes.toBytes("info"),Bytes.toBytes(hb),Bytes.toBytes(""));
            }
            else {
                put.addColumn(Bytes.toBytes("info"),Bytes.toBytes(hb),Bytes.toBytes((String) item.itemmap.get(hb)));
            }
            puts.add(put);

        }
        table.put(puts);
        table.close();
        return true;
    }


    /**
     * @Description:TODO 关闭hbase数据库连接
     * @Author: lyc
     * @Date: 2021/7/9 21:04
     * @return: void
     **/
    public void close() throws IOException {
        admin.close();
        connection.close();
    }
}
