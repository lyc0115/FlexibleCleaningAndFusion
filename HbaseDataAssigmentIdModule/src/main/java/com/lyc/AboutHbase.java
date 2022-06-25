package com.lyc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @ProjectName FlexibleCleaningAndFusion
 * @ClassName AboutHbase
 * @Description TODO
 * @Author lyc
 * @Date 2021/7/20 9:50
 **/
public class AboutHbase {

    //与HBase数据库的连接对象
    public static Connection connection;

    //数据库元数操作对象
    public static Admin admin;

    /**
     * @Description: TODO Hbase Configuration配置
     * @Author: lyc
     * @Date: 2021/7/20 9:51
     * @return: void
     **/
    public static void wakeupHbase() throws IOException {
        //取得Hbase数据库配置参数对象
        Configuration conf = HBaseConfiguration.create();
        connection = ConnectionFactory.createConnection(conf);
        admin = connection.getAdmin();
    }


    /**
     * @Description: TODO Hbase Configuration资源关闭
     * @Author: lyc
     * @Date: 2021/7/20 9:54
     * @return: void
     **/
    public static void close() throws IOException {
        admin.close();
        connection.close();
    }


    /**
     * @Description: TODO 获取Hbase表中当前id的最大值
     * @Author: lyc
     * @Date: 2021/7/20 9:55
     * @param tableName: TODO 表名
     * @return: int
     **/
    public static int getMaxHbaseRowkey(String tableName, String tableFalg) throws IOException {
        //初始化id值
        int maxid = 0;
        //获取数据表对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        //获取表中的数据
        ResultScanner scanner = table.getScanner(new Scan());
        String indexid = tableName + "ID";


        //查找当前最大的id
        for (Result result : scanner) {
            Cell[] rawCell = result.rawCells();
            for (Cell cell : rawCell) {
                if (indexid.equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
                    int num = Integer.parseInt(Bytes.toString(CellUtil.cloneValue(cell)).replace(tableFalg, ""));
                    if (num >= maxid){
                        maxid = num;
                    }
                }
            }
        }
        return maxid;
    }


    /**
     * @Description: TODO Hbase表绑定id
     * @Author: lyc
     * @Date: 2021/7/20 9:58
     * @param tablename:  TODO 绑定id的表名
     * @return: void
     **/
    public static void SetHbaseID(String tablename, String tableFlag) throws IOException {

        //获取数据表对象
        Table table = connection.getTable(TableName.valueOf(tablename));
        //获取表中的数据
        ResultScanner scanner = table.getScanner(new Scan());
        String indexID = tablename + "ID";
        int nowid = getMaxHbaseRowkey(tablename, tableFlag);
        System.out.println("最大id为："+nowid);
        //循环输出表中的数据
        for (Result result : scanner){
            Cell[] rawCell = result.rawCells();
            boolean flag = true;
            //flag=false表示已绑定id值
            for (Cell cell : rawCell) {
                if (indexID.equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
                    flag = false;
                }

                if (flag){
                    nowid += 1;
                    String key = Bytes.toString(result.getRow());
                    Put put = new Put(key.getBytes());
                    System.out.println("现在插入的id为：" + nowid);
                    put.addColumn(Bytes.toBytes("info"),Bytes.toBytes(tablename + "ID"),Bytes.toBytes(tableFlag + Integer.toString(nowid)));
                    table.put(put);
                }
            }
        }
        table.close();
    }
}
