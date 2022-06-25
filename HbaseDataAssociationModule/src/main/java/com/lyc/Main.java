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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * @ProjectName FlexibleCleaningAndFusion
 * @ClassName Main
 * @Description TODO Hbase表关联模块
 * @Author lyc
 * @Date 2021/7/30 9:26
 **/
public class Main extends Configured implements Tool {

    public static class MyMapper extends TableMapper<ImmutableBytesWritable, ImmutableBytesWritable> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String configFilePath = conf.get("configFilePath");
            String assTableA = conf.get("assTableA");
            String assTableB = conf.get("assTableB");
            String resultTable = conf.get("resultTable");
            ConfigInit.initIndexRule(configFilePath,assTableA,assTableB,resultTable);

        }

        @Override
        protected void map(ImmutableBytesWritable row, Result values, Context context) throws IOException,
                InterruptedException {
            //获取主键值
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

            if (rowkey.contains(ConfigInit.assTableName_A)) {
                Cell[] rawCell = values.rawCells();
                for (Cell cell : rawCell) {
                    qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                    cellValue = Bytes.toString(CellUtil.cloneValue(cell));
                    if (qualifier.equals(ConfigInit.tableA_ParameterA)) {
                        baseValue = cellValue;
                    }else if (qualifier.equals(ConfigInit.tableA_ParameterB)) {
                        compareValue = cellValue;
                    }

                    //避免reduce阶段键值对拆分引起数组越界，无影响可省略
                    if (cellValue.length() <= 0) {
                         cellValue = " ";
                    }

                    //拼接列名和列值形成键值对，”<=>“表示两者拼接符
                    valuestr = qualifier + "<=>" + cellValue;
                    //表名作为拼接数据的首部，对表数据进行标记，方便reduce阶段识别数据来自哪张表，“<->”表示多个列名和列值之间的拼接符
                    value += (value.length() > 0 ? "<->" : ConfigInit.assTableName_A + "<=>") + valuestr;
                }

                //此处为了方便日志查看主表数据
                System.out.println("主表值：" + value);
                //写出数据
                WriteData(context, baseValue, compareValue, value, ConfigInit.tableA_ParameterA);

            }else if (rowkey.equals(ConfigInit.assTableName_B)) {
                //副表关联值
                String assValue= "";
                //副表关联值对应id
                String assId = "";

                Cell[] rawCell = values.rawCells();
                for (Cell cell : rawCell) {
                    qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                    cellValue = Bytes.toString(CellUtil.cloneValue(cell));
                    if (qualifier.equals(ConfigInit.tableB_ParameterA)) {
                        baseValue = cellValue;
                    }else if (qualifier.equals(ConfigInit.tableB_ParameterB)) {
                        compareValue = cellValue;
                    }else if (qualifier.equals(ConfigInit.tableB_ParameterC)) {
                        assValue = cellValue;
                    }else if (qualifier.equals(ConfigInit.tableB_ParameterD)) {
                        assId = cellValue;
                    }
                }

                valuestr = ConfigInit.tableB_ParameterC + "<=>" + assValue + "<->" + ConfigInit.tableB_ParameterD + "<=>" + assId;
                //表名作为拼接数据的首部，对表数据进行标记，方便reduce阶段识别数据来自哪张表，“<->”表示多个列名和列值之间的拼接符
                value += (value.length() > 0 ? "<->" : ConfigInit.assTableName_B + "<=>") + valuestr;

                //此处为了方便日志查看副表数据
                System.out.println("副表值：" + value);
                //写出数据
                WriteData(context, baseValue, compareValue, value, ConfigInit.tableB_ParameterA);
            }
        }

        private void WriteData(Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, ImmutableBytesWritable>.Context context, String baseValue, String compareValue, String value, String table_parameterA) throws IOException, InterruptedException {
            String newRowKey;
            //对rowkey进行消歧
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
            String assTableA = conf.get("assTableA");
            String assTableB = conf.get("assTableB");
            String resultTable = conf.get("resultTable");
            ConfigInit.initIndexRule(configFilePath,assTableA,assTableB,resultTable);
        }

        @Override
        protected void reduce(ImmutableBytesWritable key, Iterable<ImmutableBytesWritable> values, Context context) throws IOException, InterruptedException {

            //获取rowkey，用于获取主副表数据
            String rowkey = Bytes.toString(key.get());
            //保存主副表数据
            ArrayList<String> assTableA = new ArrayList<>();
            ArrayList<String> assTableB = new ArrayList<>();
            //保存副表关联值和关联id，形成键值对
            HashMap<String, String> valueMap = new HashMap<>();
            //保存列名
            String qualifier = "";
            //保存列值
            String cellvalue = "";

            for (ImmutableBytesWritable value : values) {
                //获取表标记
                String tableFlag = Bytes.toString(value.get()).split("<=>")[0];
                if (tableFlag.equals(ConfigInit.assTableName_A)) {
                    //去除标记，获取表数据
                    assTableA.add(Bytes.toString(value.get()).replace(ConfigInit.assTableName_A + "<=>", ""));
                }else if (tableFlag.equals(ConfigInit.assTableName_B)){
                    //去除标记，获取表数据
                    assTableB.add(Bytes.toString(value.get()).replace(ConfigInit.assTableName_B + "<=>", ""));
                }
            }

            if (!assTableB.isEmpty()) {
                //遍历副表
                for (String rawcellB : assTableB) {
                    String mapkey = "";
                    String mapval = "";

                    String[] cellsB = rawcellB.split("<->");
                    for (String cell : cellsB) {
                        qualifier = cell.split("<=>")[0];
                        cellvalue = cell.split("<=>")[1];
                        if (qualifier.equals(ConfigInit.tableB_ParameterC)) {
                            mapkey = cellvalue;
                        }else if (qualifier.equals(ConfigInit.tableB_ParameterD)) {
                            mapval = cellvalue;
                        }
                    }
                    //形成副表关联值键值对
                    valueMap.put(mapkey, mapval);
                    //遍历主表
                    for (String rawcellA : assTableA) {
                        ArrayList<String> qualifierList = new ArrayList<>();
                        ArrayList<String> valueList = new ArrayList<>();

                        String[] cellsA = rawcellA.split("<->");
                        String[] idtostr = null;
                        String ids = "";
                        for (String cell : cellsA) {
                            qualifier = cell.split("<=>")[0];
                            cellvalue = cell.split("<=>")[1];

                            //获取主表关联值
                            if (qualifier.equals(ConfigInit.tableA_ParameterC)) {
                                idtostr = cellvalue.split(";");
                                qualifierList.add(qualifier);
                                valueList.add(cellvalue);
                            }else {
                                qualifierList.add(qualifier);
                                valueList.add(cellvalue);
                            }

                        }

                        //主表关联值不为空，遍历将关联值替换为对应的id值并拼接
                        if (idtostr != null) {
                            for (String str : idtostr) {
                                if (valueMap.containsKey(str)) {
                                    ids += (ids.length() > 0 ? ";" : "") + valueMap.get(str);
                                }
                            }
                        }
                        //添加关联后的数据
                        qualifierList.add(ConfigInit.tableAAddRelParameter);
                        valueList.add(ids);
                        //写出数据
                        WriteData(context, rowkey, qualifierList, valueList);
                    }
                }
            }else {
                for (String rawcellA : assTableA) {
                    ArrayList<String> qualifierList = new ArrayList<>();
                    ArrayList<String> valueList = new ArrayList<>();

                    String[] cellsA = rawcellA.split("<->");
                    for (String cell : cellsA) {
                        qualifier = cell.split("<=>")[0];
                        cellvalue = cell.split("<=>")[1];
                        qualifierList.add(qualifier);
                        valueList.add(cellvalue);
                    }
                    //写出数据
                    WriteData(context, rowkey, qualifierList, valueList);
                }
            }
        }

        private void WriteData(Reducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable,
                Mutation>.Context context, String rowkey, ArrayList<String> qualifierList,
                               ArrayList<String> valueList) throws IOException, InterruptedException {
            //添加主键
            String newRowKey = ConfigInit.resultTable + "<=>" + rowkey;

            Put put = new Put(Bytes.toBytes(newRowKey));
            for (int i = 0; i < qualifierList.size(); i++) {
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(qualifierList.get(i)),
                        Bytes.toBytes(valueList.get(i)));
                context.write(null, put);
            }
        }
    }


    @Override
    public int run(String[] args) throws Exception {

        boolean b = true;

        String configFilePath = args[0];
        ArrayList<String> tables = ReadConfigFile.ReadConfigItem(configFilePath, "关联Hbase表名=");

        for (String table : tables) {

            String assTableA = table.split("<=>")[0];
            String assTableB = table.split("<=>")[1];
            String resultTable = table.split("<=>")[2];
            System.out.println("关联表A(主表)：" + assTableA);
            System.out.println("关联表B(副表)：" + assTableB);
            System.out.println("结果表：" + resultTable);

            Configuration hbaseConf = HBaseConfiguration.create();
            hbaseConf.set("configFilePath", configFilePath);
            hbaseConf.set("assTableA",assTableA);
            hbaseConf.set("assTableB",assTableB);
            hbaseConf.set("resultTable",resultTable);

            Job job = Job.getInstance(hbaseConf, "association_"+table);
            job.setJarByClass(Main.class);

            ConfigInit.initIndexRule(configFilePath,assTableA,assTableB,resultTable);

            List<Scan> scans = new ArrayList<>();
            Scan scan1 = new Scan();
            scan1.setCaching(500);
            scan1.setCacheBlocks(false);
            scan1.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(assTableA));
            scans.add(scan1);

            Scan scan2 = new Scan();
            scan2.setCaching(500);
            scan2.setCacheBlocks(false);
            scan2.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(assTableB));
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
