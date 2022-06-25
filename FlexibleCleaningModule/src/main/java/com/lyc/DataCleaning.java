package com.lyc;

import java.io.Serializable;

/**
 * @ProjectName FlexibleCleaningAndFusion
 * @ClassName DataCleaning
 * @Description TODO
 * @Author lyc
 * @Date 2021/7/10 9:55
 * @Version 1.0
 **/
public class DataCleaning implements Serializable {

    //清洗数据
    DataCleanRule cleanData;

    //清洗规则
    String ruleStr="";

    public DataCleaning(String valueStr,String cleanRule){
        cleanData=new DataCleanRule(valueStr);
        ruleStr=cleanRule;
    }

    /**
     * @Description: TODO 清洗标准，根据以后业务需要动态调整
     * @Author: lyc
     * @Date: 2022/2/26 11:02
     * @return: java.lang.Boolean
     **/
    public Boolean cleanDataByRule(){
        //?_#11
        String[] ruleSet=ruleStr.split("#");
        //11
        for(int i=0;i<ruleSet.length;i++){

            String rule=ruleSet[i];
            String choice=rule.substring(0,2);

            //使用规则一：替换规则
            if(choice.equals("01")){
                String oldStr="";
                String newStr="";
                String[] splitStr=rule.split("_");
                //默认为消去字符串中的某个字符
                if(splitStr.length==2){
                    oldStr=splitStr[1];
                    newStr="";
                }
                else{
                    oldStr=splitStr[1];
                    newStr=splitStr[2];
                }
                cleanData.rule1(oldStr, newStr);
            }
            //使用规则二：清除数字规则
            else if(choice.equals("02")){
                cleanData.rule2();
            }
            //使用规则三：清除字母规则
            else if(choice.equals("03")){
                cleanData.rule3();
            }
            //使用规则四：清除两字符之间的信息
            else if(choice.equals("04")){
                String charL=rule.split("_")[1];
                String charR=rule.split("_")[2];
                cleanData.rule4(charL, charR);
            }
            //使用规则五：获取某个字符之前的字符串
            else if(choice.equals("05")){
                String splitChar=rule.split("_")[1];
                cleanData.rule5(splitChar);
            }
            //使用规则六：检查email的格式
            else if(choice.equals("06")){
                cleanData.rule6();
            }
            //使用规则七：检查mobile的格式
            else if(choice.equals("07")){
                //cleanData.rule7();
            }
            //使用规则八：长度不满足要求的字段值置空
            else if(choice.equals("08")){
                String judgeChar=rule.split("_")[1];
                int strLen=Integer.parseInt(rule.split("_")[2]);
                cleanData.rule8(judgeChar,strLen);
            }
            // 规则九：获取某个字符或字符串之后的信息
            else if(choice.equals("09")){
                String splitChar=rule.split("_")[1];
                cleanData.rule9(splitChar);
            }
            // 规则十：规范组织格式
            else if(choice.equals("10")){
                //cleanData.rule10();
            }
            // 规则十一：替换大部分空白字符(\s 可以匹配空格、制表符、换页符等空白字符的其中任意一个)
            else if(choice.equals("11")){
                cleanData.rule11();
            }
            // 规则十二：获取字符串中的汉字
            else if(choice.equals("12")){
                cleanData.rule12();
            }
            // 规则十三：规范人名，中文名和英文名
            else if(choice.equals("13")){
                cleanData.rule13();
            }
            //清晰规则使用有误
            else{
                return false;
            }
        }
        return true;
    }

    public String getCleanResult(){

        return cleanData.getStr();
    }
}
