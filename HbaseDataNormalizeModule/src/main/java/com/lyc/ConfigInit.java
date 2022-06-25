package com.lyc;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;

/**
 * @ProjectName FlexibleCleaningAndFusion
 * @ClassName com.lyc.com.lyc.ConfigInit
 * @Description TODO
 * @Author lyc
 * @Date 2021/7/12 9:43
 **/
public class ConfigInit implements Serializable {

    //表合并参数规则
    public static ArrayList<String> mergeRules;

    public static ArrayList<String> mergeRuleList;

    //表合并增加列限定符名
    public static String mergeExtendCols;

    public static void initIndexRule(String filePath, String tableName) throws IOException {
        mergeRules = ReadConfigFile.ReadConfigItem(filePath, "表属性合并规则=");
        mergeRuleList = new ArrayList<String>();
        for (String mergeRule : mergeRules) {
            String rule = mergeRule.substring(mergeRule.indexOf(")") + 1);
            mergeRuleList.add(rule);
        }
        mergeExtendCols = ReadConfigFile.ReadConfigItem(filePath, "表属性合并附加列=").get(0);

    }
}
