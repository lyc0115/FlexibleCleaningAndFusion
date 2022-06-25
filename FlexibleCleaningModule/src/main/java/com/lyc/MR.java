package com.lyc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @ProjectName FlexibleCleaningAndFusion
 * @ClassName MR
 * @Description TODO 数据清洗主入口
 * @Author lyc
 * @Date 2021/7/10 16:17
 * @Version 1.0
 **/
public class MR extends Configured implements Tool {


    /**
     * @Description: MyMapper函数
     * @Author: lyc
     * @Date: 2021/7/10 16:34
     * @return: null
     **/
    public static class MyMapper extends TableMapper<Text, Put> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            //在运行开始前，将各类参数写入到MR框架中的Config文件中进行参数传递
            Configuration conf = context.getConfiguration();

            String filePath = conf.get("filePath");
            String Hbasetablename = conf.get("Hbasetablename");
            ConfigInit.initIndexRule(filePath, Hbasetablename);
            ConfigInit.outputtable = conf.get("Outputtable");
        }


        @Override
        protected void map(ImmutableBytesWritable row, Result values, Context context) throws IOException,
                InterruptedException {
            //对主键进行处理
            String rowkey = Bytes.toString(row.get());
            //输出表名拼接rowkey生成新的rowkey，以表名做区分标志，方便后续操作，根据实际情况可更改
            rowkey = ConfigInit.outputtable + "<=>" + rowkey;
            Put put = new Put(Bytes.toBytes(rowkey));
            Cell rawCell[] = values.rawCells();
            //遍历数据
            for (Cell cell : rawCell) {
                //提取运行配置文件中的属性索引，与hbase表中列名进行对比，相同模块提取属性值与清洗规则后进行清洗
                //获取列名
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                //tableindex存储列旧属性
                if (ConfigInit.tableindex.contains(qualifier)) {
                    //获取列值
                    String cellvalue = Bytes.toString(CellUtil.cloneValue(cell));
                    System.out.println(qualifier + ":" + cellvalue);
                    if (!cellvalue.isEmpty()) {
                        //遍历清洗规则
                        for (String rule : ConfigInit.indexcleanrule) {
                            //若清洗规则包含对列名的清洗格式，则进行根据配置文件进行清洗操作
                            if (rule.contains(qualifier)) {
                                rule = rule.substring(rule.indexOf("\"") + 1, rule.lastIndexOf("\""));
                                DataCleaning cleanData = new DataCleaning(cellvalue, rule);
                                if (cleanData.cleanDataByRule()) {
                                    cellvalue = cleanData.getCleanResult();
                                }
                            }
                        }
                        System.out.println(cellvalue);

                        if (cellvalue.length() > 0) {
                            put.addColumn(Bytes.toBytes(ConfigInit.columnfamily),
                                    Bytes.toBytes(ConfigInit.oldIndexToNew.get(qualifier)), Bytes.toBytes(cellvalue));
                        }
                    }

                }
                if (!put.isEmpty()) {
                    context.write(new Text(rowkey), put);
                }

            }

        }
    }

    /**
     * @Description: MyReducer函数
     * @Author: lyc
     * @Date: 2021/7/10 16:34
     * @return: null
     **/
    public static class MyReducer extends TableReducer<Text, Put, ImmutableBytesWritable> {
        @Override
        protected void reduce(Text key, Iterable<Put> values, Context context) throws IOException,
                InterruptedException {
            //遍历写出数据
            for (Put value : values) {
                context.write(null, value);
            }
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        boolean b = true;
        String filepath = args[0];
        ArrayList<String> tables = ReadConfigFile.ReadConfigItem(filepath, "清洗Hbase表名=");
        for (String table : tables) {
            //被清洗的表名
            String inputtable = table.split("<=>")[0];
            //清洗后的表名
            String outputtable = table.split("<=>")[1];
            Configuration hbaseConf = HBaseConfiguration.create();
            //初始化配置信息路径、清洗表表名(inputtable)、清洗后表名(outputtable)
            hbaseConf.set("filePath", filepath);
            hbaseConf.set("Hbasetablename", inputtable);
            hbaseConf.set("Outputtable", outputtable);

            //设置job名称
            Job job = Job.getInstance(hbaseConf, "clean_" + table);
            job.setJarByClass(MR.class);
            TableMapReduceUtil.initTableMapperJob(inputtable, new Scan(), MyMapper.class, Text.class, Put.class, job);
            TableMapReduceUtil.initTableReducerJob(outputtable, MyReducer.class, job);

            b = job.waitForCompletion(true);
            return 0;
        }
        return b ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        //创建HBaseConfiguration配置
        Configuration configuration = HBaseConfiguration.create();
        int run = ToolRunner.run(configuration, new MR(), args);
        System.exit(run);
    }
}
