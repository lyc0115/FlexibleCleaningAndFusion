package com.lyc;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * @ProjectName FlexibleCleaningAndFusion
 * @ClassName ConfigInit
 * @Description TODO
 * @Author lyc
 * @Date 2021/7/10 9:53
 * @Version 1.0
 **/
public class ConfigInit  implements Serializable {
    //存储要读取hbase表中列名
    static ArrayList<String> tableindex = null;
    //存储列名对应的清洗规则
    static ArrayList<String> indexcleanrule =null;
    //输出表名
    static String outputtable = null;
    //表列族名
    static String columnfamily = null;
    //新表列名替换旧表名，旧表列名与新表列名映射关系
    static HashMap<String,String> oldIndexToNew = new HashMap<String, String>();

    public static void initIndexRule(String filepath,String tablename) throws IOException {

        tableindex=new ArrayList<String>();
        indexcleanrule=new ArrayList<String>();

        //从配置文件获取清新表的每个列属性的清洗规则
        indexcleanrule = ReadConfigFile.ReadConfigItem(filepath,tablename+"表属性清洗规则=");

        columnfamily = ReadConfigFile.ReadConfigItem(filepath,tablename+"表列族名=").get(0);
        for (String indexs: ReadConfigFile.ReadConfigItem(filepath,tablename+"表旧属性与新属性对应关系=")){
            String oldindex = indexs.split("<=>")[0];
            tableindex.add(oldindex);
            String newindex = indexs.split("<=>")[1];
            oldIndexToNew.put(oldindex,newindex);
        }
    }
}
