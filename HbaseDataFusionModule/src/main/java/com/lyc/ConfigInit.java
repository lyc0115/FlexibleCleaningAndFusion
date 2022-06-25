package com.lyc;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * @ProjectName FlexibleCleaningAndFusion
 * @ClassName ConfigInit
 * @Description TODO 主表为原始表，副表为补充原始表信息的表，抽取副表中的属性信息对原始表数据进行扩充或补充说明，完善原始表数据
 * @Author lyc
 * @Date 2021/7/30 9:18
 **/
public class ConfigInit implements Serializable {
    //融合主表表名
    static String fusionTableName_A;
    //融合副表表名
    static String fusionTableName_B;
    //结果表名
    static String resultTable;

    //主表融合参数A
    static String tableA_ParameterA;
    //主表融合参数B
    static String tableA_ParameterB;
    //副表融合参数A
    static String tableB_ParameterA;
    //副表融合参数B
    static String tableB_ParameterB;
    //副表融合参数C
    static String tableB_ParameterC;
    //融合后主表增加的关系参数
    static String tableAAddRelParameter;

    //主表与副表属性对应关系
    static HashMap<String,String> oldToNewMap = new HashMap<>();

    //副表提取属性集合
    static ArrayList<String> extractParameters = new ArrayList<>();

    public static void initIndexRule(String filePath, String tableNameA, String tableNameB, String resultTableName) throws IOException {

        fusionTableName_A = tableNameA;

        fusionTableName_B = tableNameB;

        resultTable = resultTableName;

        tableA_ParameterA = ReadConfigFile.ReadConfigItem(filePath, tableNameA + "<=>" + tableNameB + "主表融合参数=").get(0).split("<=>")[0];

        tableA_ParameterB = ReadConfigFile.ReadConfigItem(filePath, tableNameA + "<=>" + tableNameB + "主表融合参数=").get(0).split("<=>")[1];

        tableB_ParameterA = ReadConfigFile.ReadConfigItem(filePath, tableNameA + "<=>" + tableNameB + "副表融合参数=").get(0).split("<=>")[0];

        tableB_ParameterB = ReadConfigFile.ReadConfigItem(filePath, tableNameA + "<=>" + tableNameB + "副表融合参数=").get(0).split("<=>")[1];

        tableB_ParameterC = ReadConfigFile.ReadConfigItem(filePath, tableNameA + "<=>" + tableNameB + "副表融合参数=").get(0).split("<=>")[2];

        tableAAddRelParameter = ReadConfigFile.ReadConfigItem(filePath, tableNameA + "<=>" + tableNameB  + "表融合附加关系参数=").get(0);


        ArrayList<String> list = ReadConfigFile.ReadConfigItem(filePath, tableNameA + "<=>" + tableNameB + "主表与副表属性对应关系=");



        for (String indexs : list) {
            if (indexs.length() > 0) {
                String oldindex = indexs.split("<=>")[0];
                String newindex = indexs.split("<=>")[1];
                extractParameters.add(oldindex);
                oldToNewMap.put(oldindex, newindex);
            }

        }

    }

}
