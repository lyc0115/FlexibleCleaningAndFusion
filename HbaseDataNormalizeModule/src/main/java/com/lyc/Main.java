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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

/**
 * @ProjectName FlexibleCleaningAndFusion
 * @ClassName com.lyc.com.lyc.Main
 * @Description TODO 多源异构数据标准化（如人才，组织数据来自不同业务系统，统一数据标准）
 * @Author lyc
 * @Date 2021/7/12 8:43
 **/
public class Main extends Configured implements Tool {

    public static class MyMapper extends TableMapper<ImmutableBytesWritable, Put> {
        public Configuration conf = null;
        public String filePath = null;
        public String inputtable = null;
        public String outputtable = null;

        @Override
        protected void setup(Mapper.Context context) throws IOException {
            //在运行开始前，将各类参数写入到MR框架中的Config文件中进行参数传递
            conf = context.getConfiguration();
            filePath = conf.get("filepath");
            inputtable = conf.get("inputtable");
            outputtable = conf.get("outputtable");
            ConfigInit.initIndexRule(filePath, outputtable);
        }

        @Override
        protected void map(ImmutableBytesWritable row, Result values, Mapper.Context context) throws IOException,
                InterruptedException {
            //存储单个列限定符名
            String qualifier = "";
            //存储单个列值
            String cellvalue = "";
            //列值集合
            ArrayList<String> valueSet = new ArrayList<>();
            //列限定符值集合
            ArrayList<String> qualifierSet = new ArrayList<>();

            //将配置文件的字段合并规则存储至mergeRuleMap形成键值对
            HashMap<String, ArrayList<String>> mergeRuleMap = new HashMap<>();
            for (String rule : ConfigInit.mergeRuleList) {
                ArrayList<String> mergeRule = new ArrayList(Arrays.asList(rule.split("<=>")));
                String mergeRuleKey = mergeRule.get(0);
                mergeRule.remove(0);
                mergeRuleMap.put(mergeRuleKey, mergeRule);
            }
            //打印结果，日志中查看验证是否为预期结果
            System.out.println("合并规则：" + mergeRuleMap);

            //遍历表中的每一个列限定符
            Cell[] rawCell = values.rawCells();
            for (Cell cell : rawCell) {
                //记录合并标志，若为true，该字段需要合并，若为false，该字段不做处理
                boolean flag = false;
                //获取列限定符名
                qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                //获取列值
                cellvalue = Bytes.toString(CellUtil.cloneValue(cell));

                //记录当前合并的列限定符名
                String mergeRuleValue = "";
                for (String mergeRuleKey : mergeRuleMap.keySet()) {
                    //判断map值集合中是否包含列限定符名
                    if (mergeRuleMap.get(mergeRuleKey).contains(qualifier)){
                        flag = true;
                        mergeRuleValue = mergeRuleKey;
                    }
                }

                System.out.println("合并后key：" + mergeRuleValue);

                //flag=true，合并标准化操作执行
                if (flag) {
                    qualifierSet.add(mergeRuleValue);
                    valueSet.add(cellvalue);
                }else {
                    qualifierSet.add(qualifier);
                    valueSet.add(cellvalue);
                }
            }
            //以下11行根据业务添加的附属列
            //添加rowkey值，此处为id
            qualifierSet.add("id");
            valueSet.add(Bytes.toString(row.get()));
            //数据来源字段
            String mergeColA= ConfigInit.mergeExtendCols.split("<=>")[0];
            //数据标识字段
            String mergeColB = ConfigInit.mergeExtendCols.split("<=>")[1];
            //添加数据来源
            qualifierSet.add(mergeColA);
            //以表名作为标识
            valueSet.add(inputtable);
            //标识数据是哪类数据
            qualifierSet.add(mergeColB);
            //以表名作为标识
            valueSet.add(inputtable);

            //打印结果，验证是否为预期效果
            for (int i = 0; i < qualifierSet.size(); i++) {
                System.out.println("列限定符=" + qualifierSet.get(i) + "，列值=" + valueSet.get(i));
            }

            //生成主键
            String rowkey = inputtable + "<=>" + Bytes.toString(row.get());
            ImmutableBytesWritable key = new ImmutableBytesWritable(Bytes.toBytes(rowkey));
            //将数据写出
            Put put = new Put(Bytes.toBytes(rowkey));
            for (int i = 0; i < qualifierSet.size(); i++) {
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(qualifierSet.get(i)), Bytes.toBytes(valueSet.get(i)));
                context.write(key, put);
            }
        }
    }


    public static class MyReducer extends TableReducer<ImmutableBytesWritable, Put, NullWritable> {
        @Override
        protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException,
                InterruptedException {
            for (Put value : values) {
                context.write(NullWritable.get(), value);
            }
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        boolean result = true;
        //配置文件路径
        String filePath = args[0];
        //获取合并hbase表名集合（多个）
        ArrayList<String> mergeTables = ReadConfigFile.ReadConfigItem(filePath, "合并Hbase表集合=");
        //遍历每一个合并hbase表名集合
        for (String mergeTable : mergeTables) {
            //将合并hbase表名集合中的每一个表名存储到arraylist，方便处理
            ArrayList<String> tables = new ArrayList(Arrays.asList(mergeTable.split("<=>")));
            //获取合并后输出表，列表索引0处对应合并后输出的表名
            String outputtable = tables.remove(0);
            //遍历需要合并的表名
            for (String inputtable : tables) {
                //配置Hbase相关信息
                Configuration hbaseconf = HBaseConfiguration.create();
                hbaseconf.set("filepath", filePath);
                hbaseconf.set("inputtable", inputtable);
                hbaseconf.set("outputtable", outputtable);

                Job job = Job.getInstance(hbaseconf, "fusion_" + inputtable);
                job.setJarByClass(Main.class);

                ConfigInit.initIndexRule(filePath, outputtable);

                TableMapReduceUtil.initTableMapperJob(inputtable, new Scan(), MyMapper.class, ImmutableBytesWritable.class, Put.class, job);
                TableMapReduceUtil.initTableReducerJob(outputtable, MyReducer.class, job);
                result = job.waitForCompletion(true);
            }
        }
        return result ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        int run = ToolRunner.run(configuration, new Main(), args);
        System.exit(run);
    }
}
