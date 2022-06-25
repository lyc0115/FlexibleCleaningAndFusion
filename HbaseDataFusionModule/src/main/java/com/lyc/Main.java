package com.lyc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ProjectName FlexibleCleaningAndFusion
 * @ClassName Main
 * @Description TODO
 * @Author lyc
 * @Date 2021/7/30 9:26
 **/
public class Main extends Configured implements Tool {

    public static class MyMapper extends TableMapper<ImmutableBytesWritable, ImmutableBytesWritable> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String configFilePath = conf.get("configFilePath");
            String fusionTableA = conf.get("fusionTableA");
            String fusionTableB = conf.get("fusionTableB");
            String resultTable = conf.get("resultTable");
            ConfigInit.initIndexRule(configFilePath,fusionTableA,fusionTableB,resultTable);

        }

        @Override
        protected void map(ImmutableBytesWritable row, Result values, Context context) throws IOException,
                InterruptedException {
            //获取主键
            String rowkey = Bytes.toString(row.get());
            //保存列标识符（列名）
            String qualifier = "";
            //保存列值
            String cellValue = "";
            //保存基本属性
            String baseValue = "";
            //保存对比属性
            String compareValue = "";
            //保存拼接列名和列值
            String valuestr = "";
            //拼接保存表中所有数据
            String value = "";
            //定义新的rowkey
            String newRowKey = "";

            //根据主键的区分标志分别进行处理，以融合标准将数据进入reduce阶段进行融合
            if (rowkey.contains(ConfigInit.fusionTableName_A)) {
                Cell[] rawCell = values.rawCells();
                for (Cell cell : rawCell) {
                    qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                    cellValue = Bytes.toString(CellUtil.cloneValue(cell));
                    //获取融合标准所需要的字段
                    if (qualifier.equals(ConfigInit.tableA_ParameterA)) {
                        baseValue = cellValue;
                    } else if (qualifier.equals(ConfigInit.tableA_ParameterB)){
                        compareValue = cellValue;
                    }

                    //避免reduce阶段键值对拆分引起数组越界，无影响可省略
                    if (cellValue.length() <= 0) {
                        cellValue = " ";
                    }
                    //拼接列名和列值形成键值对，”<=>“表示两者拼接符
                    valuestr = qualifier + "<=>" + cellValue;
                    //表名作为拼接数据的首部，对表数据进行标记，方便reduce阶段识别数据来自哪张表，“<->”表示多个列名和列值之间的拼接符
                    value += (value.length() > 0 ? "<->" : ConfigInit.fusionTableName_A + "<=>") + valuestr;
                }
                System.out.println("主表值：" + value);

                //消除主键歧义
                WriteData(context, baseValue, compareValue, value, ConfigInit.tableA_ParameterA);
            }else if (rowkey.contains(ConfigInit.fusionTableName_B)) {
                Cell[] rawCell = values.rawCells();
                for (Cell cell : rawCell) {
                    qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                    cellValue = Bytes.toString(CellUtil.cloneValue(cell));

                    if (qualifier.equals(ConfigInit.tableB_ParameterA)) {
                        baseValue = cellValue;
                    }else if (qualifier.equals(ConfigInit.tableB_ParameterB)) {
                        compareValue = cellValue;
                    }

                    //提取副表和主表相关的属性
                    if (ConfigInit.extractParameters.contains(qualifier)) {
                        //避免reduce阶段键值对拆分引起数组越界，无影响可省略
                        if (cellValue.length() <= 0) {
                            cellValue = " ";
                        }
                        valuestr = qualifier + "<=>" + cellValue;
                        value += (value.length() > 0 ? "<->" : ConfigInit.fusionTableName_B + "<=>") + valuestr;
                    }
                }

                System.out.println("副表值：" + value);
                WriteData(context, baseValue, compareValue, value, ConfigInit.tableB_ParameterA);

            }
        }

        private void WriteData(Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, ImmutableBytesWritable>.Context context, String baseValue, String compareValue, String value, String table_parameterA) throws IOException, InterruptedException {
            String newRowKey;//消除主键歧义
            if (table_parameterA.length() == 0) {
                newRowKey = compareValue;
            } else {
                newRowKey = baseValue + ":" + compareValue;
            }

            ImmutableBytesWritable keyv =
                    new ImmutableBytesWritable(Bytes.toBytes(newRowKey));
            ImmutableBytesWritable val = new ImmutableBytesWritable(Bytes.toBytes(value));
            context.write(keyv, val);
        }
    }

    public static class MyReducer extends TableReducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String configFilePath = conf.get("configFilePath");
            String fusionTableA = conf.get("fusionTableA");
            String fusionTableB = conf.get("fusionTableB");
            String resultTable = conf.get("resultTable");
            ConfigInit.initIndexRule(configFilePath,fusionTableA,fusionTableB,resultTable);
        }

        @Override
        protected void reduce(ImmutableBytesWritable key, Iterable<ImmutableBytesWritable> values, Context context) throws IOException, InterruptedException {

            String rowkey = Bytes.toString(key.get());
            //存储主副表数据
            ArrayList<String> fusionTableA = new ArrayList<>();
            ArrayList<String> fusionTableB = new ArrayList<>();
            //保存列名
            String qualifier = "";
            //保存列值
            String cellvalue = "";
            HashMap<String, String> valueMap = new HashMap<>();

            //根据区分标志，将数据分别存入对应的集合
            for (ImmutableBytesWritable value : values) {
                //获取表标记
                String tableFlag = Bytes.toString(value.get()).split("<=>")[0];
                if (tableFlag.equals(ConfigInit.fusionTableName_A)) {
                    fusionTableA.add(Bytes.toString(value.get()).replace(tableFlag + "<=>", ""));
                }else if (tableFlag.equals(ConfigInit.fusionTableName_B)){
                    fusionTableB.add(Bytes.toString(value.get()).replace(tableFlag + "<=>", ""));
                }
            }

            if (!fusionTableB.isEmpty()) {
                String fusionValue = "";
                for (String rawcell : fusionTableB) {
                    String[] cells = rawcell.split("<->");
                    for (String cell : cells) {
                        qualifier = cell.split("<=>")[0];
                        cellvalue = cell.split("<=>")[1];
                        //以专利为例，提取副表中所有的专利名
                        if (qualifier.equals(ConfigInit.tableAAddRelParameter)) {
                            fusionValue += (fusionValue.length() > 0 ? ";" : "") + cellvalue;
                        }
                        //将副表提取的列名标准化主表列名
                        valueMap.put(ConfigInit.oldToNewMap.get(qualifier), cellvalue);
                    }
                }
                //保存所有专利名
                valueMap.put(ConfigInit.tableAAddRelParameter, fusionValue);
                //写出数据
                WriteData(context, valueMap, rowkey, fusionTableA);
            }else {
                WriteData(context, valueMap, rowkey, fusionTableA);
            }

        }

        public void WriteData(Reducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, Mutation>.Context context, HashMap<String, String> valueMap, String rowkey, ArrayList<String> fusionTableA) throws IOException, InterruptedException {
            String qualifier = "";
            String cellvalue = "";
            for (String rawcell : fusionTableA) {
                String[] cells = rawcell.split("<->");
                for (String cell : cells) {
                    qualifier = cell.split("<=>")[0];
                    cellvalue = cell.split("<=>")[1];
                    valueMap.put(qualifier, cellvalue);
                }
            }
            String newRowKey = ConfigInit.resultTable + "<=>" + rowkey;
            //添加主键
            Put put = new Put(Bytes.toBytes(newRowKey));
            ImmutableBytesWritable key = new ImmutableBytesWritable(Bytes.toBytes(newRowKey));
            //添加融合后数据
            for (Map.Entry<String, String> entry : valueMap.entrySet()) {
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
                context.write(key, put);
            }
        }
    }


    @Override
    public int run(String[] args) throws Exception {

        boolean b = true;

        String configFilePath = args[0];
        ArrayList<String> tables = ReadConfigFile.ReadConfigItem(configFilePath, "融合Hbase表名=");

        for (String table : tables) {

            String fusionTableA = table.split("<=>")[0];
            String fusionTableB = table.split("<=>")[1];
            String resultTable = table.split("<=>")[2];
            System.out.println("融合表A(主表)：" + fusionTableA);
            System.out.println("融合表B(副表)：" + fusionTableB);
            System.out.println("结果表：" + resultTable);

            Configuration hbaseConf = HBaseConfiguration.create();
            hbaseConf.set("configFilePath", configFilePath);
            hbaseConf.set("fusionTableA", fusionTableA);
            hbaseConf.set("fusionTableB",fusionTableB);
            hbaseConf.set("resultTable",resultTable);

            Job job = Job.getInstance(hbaseConf, "fusion_"+fusionTableA + "_" + fusionTableB);
            job.setJarByClass(Main.class);

            ConfigInit.initIndexRule(configFilePath,fusionTableA,fusionTableB,resultTable);

            List<Scan> scans = new ArrayList<>();
            Scan scan1 = new Scan();
            scan1.setCaching(500);
            scan1.setCacheBlocks(false);
            scan1.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(fusionTableA));
            scans.add(scan1);

            Scan scan2 = new Scan();
            scan2.setCaching(500);
            scan2.setCacheBlocks(false);
            scan2.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(fusionTableB));
            scans.add(scan2);

            TableMapReduceUtil.initTableMapperJob(scans, MyMapper.class, ImmutableBytesWritable.class, ImmutableBytesWritable.class, job);
            TableMapReduceUtil.initTableReducerJob(resultTable, MyReducer.class, job);

            b = job.waitForCompletion(true);


        }
        return b ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        //创建HBaseConfiguration配置
        Configuration configuration = HBaseConfiguration.create();
        int run = ToolRunner.run(configuration, new Main(), args);
        System.exit(run);
    }
}
