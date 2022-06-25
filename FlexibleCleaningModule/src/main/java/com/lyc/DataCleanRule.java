package com.lyc;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @ProjectName FlexibleCleaningAndFusion
 * @ClassName DataCleanRule 清洗规则配置类
 * @Description TODO
 * @Author lyc
 * @Date 2021/7/10 9:54
 * @Version 1.0
 **/
public class DataCleanRule {
    private String str = "";

    public DataCleanRule(String str) {
        this.str = str;
    }

    // 规则一：字符串中的oldStr用newStr替换
    public void rule1(String oldStr, String newStr) {
        //There are reserved character in Regex.比如："?" 应该"\\?"
        //str.replaceAll(".", newStr)将锁喉字符全部替换成newStr."\\."为替换字符"."
        str = str.replaceAll(oldStr, newStr);
    }

    // 规则二：去除字符串中的数字
    public void rule2() {
        Pattern p = Pattern.compile("[0-9\\.]+");
        Matcher m = p.matcher(str);
        while (m.find()) {
            String intStr = m.group();
            str = str.replace(intStr, "");
        }
    }

    // 规则三：去除字符串中的字母
    public void rule3() {
        Pattern p = Pattern.compile("[a-z\\.]+");
        Matcher m = p.matcher(str.toLowerCase());
        while (m.find()) {
            String charStr = m.group();
            str = str.replace(charStr, "");
        }
    }

    // 规则四：去除字符串中某两个字符之间的信息（建议成对出现的字符）
    public  String rule4(String charL, String charR) {

        while (str.contains(charL)) {
            if (str.contains(charR)) {
                int left = str.indexOf(charL);
                int right = str.indexOf(charR);
                if (left <= right) {
                    String subStr = str.substring(left, right + 1);
                    str = str.replace(subStr, "");
                }else {
                    str = "";
                }
            }else {
                str = "";
            }
        }

        while (str.contains(charR)) {
            if (str.contains(charL)) {
                int left = str.indexOf(charL);
                int right = str.indexOf(charR);
                if (left <= right) {
                    String subStr = str.substring(left, right + 1);
                    str = str.replace(subStr, "");
                }else {
                    str = "";
                }
            }else {
                str = "";
            }
        }
        return str;
    }

    // 规则五：获取某个字符或字符串之前的信息
    public void rule5(String splitChar) {
        if (str.contains(splitChar)) {
            int position = str.indexOf(splitChar);
            str = str.substring(0, position);
        }
    }

    // 规则六：规范email的格式
    public void rule6() {
        String newStr = "";
        //email的正则表达式
        String regex = "\\w+(\\.\\w)*@\\w+(\\.\\w{2,3}){1,3}";
        String[] splitStr = str.split(";");
        for (String emailStr:splitStr){
            if(emailStr.matches(regex)){
                newStr += (newStr.length() > 0 ? ";" : "") + emailStr;
            }
        }
        if (newStr.length() == 0) {
            newStr = "null";
        }
        str = newStr;
    }

    // 规则八：不满足一定数目的字段值置空
    public void rule8(String judgeChar, int strLen) {
        if(judgeChar.equals("=")){
            if (str.length() == strLen) {
                str = "null";
            }
        }
        else if(judgeChar.equals("《")){
            if (str.length() < strLen) {
                str = "null";
            }
        }
        else if(judgeChar.equals("》")){
            if (str.length() > strLen) {
                str = "null";
            }
        }
    }

    // 规则九：获取某个字符或字符串之后的信息
    public void rule9(String splitChar) {
        if (str.contains(splitChar)) {
            int position = str.indexOf(splitChar);
            int strLen=splitChar.length();
            str = str.substring(position+strLen);
        }

    }

//    // 规则十：规范组织格式（把握组织粒度！！！）
//    public void rule10() {
//        Organization orgStr=new Organization(str);
//        str=orgStr.handleOrg();
//    }

    // 规范人名可以组合规则十一和规则十二
    // 规则十一：替换大部分空白字符(\s 可以匹配空格、制表符、换页符等空白字符的其中任意一个)
    public void rule11() {

        str = str.replaceAll("\\s*", "");
    }

    // 规则十二：替换字符串中的汉字
    public void rule12() {

        str = str.replaceAll("[^\u4e00-\u9fa5]", "");
    }

    // 规则十三：规范人名，中文名
    // 注意其他字段置空处理：str="null"，人名置空处理：str=""
    public void rule13() {

        String[] nameSet=str.split(";|，|,");
        String newStr="";
        for(String name:nameSet){
            name = name.replaceAll("[^\u4e00-\u9fa5]", "");
            if(name.length()>1 && name.length()<5){
                newStr+=(newStr.length()>0?";":"")+name;
            }
        }
        if(newStr.length() == 0){
            newStr="无";
        }
        str=newStr;
    }

    public String getStr() {
        return str;
    }


}
