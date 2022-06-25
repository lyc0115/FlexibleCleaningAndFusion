package com.lyc;

import java.io.IOException;
import java.io.Serializable;

/**
 * @ProjectName FlexibleCleaningAndFusion
 * @ClassName ConfigInit
 * @Description TODO
 * @Author lyc
 * @Date 2021/7/30 9:18
 **/
public class ConfigInit implements Serializable {
    //关联主表表名
    static String assTableName_A;
    //关联副表表名
    static String assTableName_B;
    //输出表表名
    static String resultTable;

    //主表关联参数A
    static String tableA_ParameterA;
    //主表关联参数B
    static String tableA_ParameterB;
    //主表关联参数C
    static String tableA_ParameterC;
    //副表关联参数A
    static String tableB_ParameterA;
    //副表关联参数B
    static String tableB_ParameterB;
    //副表关联参数C
    static String tableB_ParameterC;
    //副表关联参数D
    static String tableB_ParameterD;
    //关联后主表增加的关系参数
    static String tableAAddRelParameter;



    public static void initIndexRule(String filePath, String tableNameA, String tableNameB, String resultTableName) throws IOException {

        assTableName_A = tableNameA;

        assTableName_B = tableNameB;

        resultTable = resultTableName;

        tableA_ParameterA = ReadConfigFile.ReadConfigItem(filePath, tableNameA + "<=>" + tableNameB + "主表关联参数=").get(0).split("<=>")[0];

        tableA_ParameterB = ReadConfigFile.ReadConfigItem(filePath, tableNameA + "<=>" + tableNameB + "主表关联参数=").get(0).split("<=>")[1];

        tableA_ParameterC = ReadConfigFile.ReadConfigItem(filePath, tableNameA + "<=>" + tableNameB + "主表关联参数=").get(0).split("<=>")[2];

        tableB_ParameterA = ReadConfigFile.ReadConfigItem(filePath, tableNameA + "<=>" + tableNameB + "副表关联参数=").get(0).split("<=>")[0];

        tableB_ParameterB = ReadConfigFile.ReadConfigItem(filePath, tableNameA + "<=>" + tableNameB + "副表关联参数=").get(0).split("<=>")[1];

        tableB_ParameterC = ReadConfigFile.ReadConfigItem(filePath, tableNameA + "<=>" + tableNameB + "副表关联参数=").get(0).split("<=>")[2];

        tableB_ParameterD = ReadConfigFile.ReadConfigItem(filePath, tableNameA + "<=>" + tableNameB + "副表关联参数=").get(0).split("<=>")[3];

        tableAAddRelParameter = ReadConfigFile.ReadConfigItem(filePath, tableNameA + "<=>" + tableNameB  + "表关联附加关系参数=").get(0);


    }

}
