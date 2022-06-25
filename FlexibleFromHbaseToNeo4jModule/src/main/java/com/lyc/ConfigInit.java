package com.lyc;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @ProjectName FlexibleCleaningAndFusion
 * @ClassName ConfigInit
 * @Description TODO
 * @Author lyc
 * @Date 2021/8/7 14:36
 **/
public class ConfigInit {
    static String url = null;
    static String username = null;
    static String passwd = null;
    static ArrayList<String> tablename = null;
    static ArrayList<String> indexRel = null;
    static ArrayList<String> globalkeyvalue = null;
    public static void initIndexRule(String filepath) throws IOException {
        url = ReadConfigFile.ReadConfigItem(filepath,"Neo4j数据库url=").get(0);
        System.out.println("url：" + url);
        username = ReadConfigFile.ReadConfigItem(filepath,"Neo4j数据库用户名=").get(0);
        System.out.println("username：" + username);
        passwd = ReadConfigFile.ReadConfigItem(filepath,"Neo4j数据库密码=").get(0);
        System.out.println("passwd：" + passwd);
        tablename = ReadConfigFile.ReadConfigItem(filepath,"转化表名=");
        System.out.println("tablename：" + tablename);
        indexRel = ReadConfigFile.ReadConfigItem(filepath,"表属性关系=");
        System.out.println("indexRel：" + indexRel);
        globalkeyvalue = ReadConfigFile.ReadConfigItem(filepath,"Neo4j节点添加全局属性=");
    }
}
