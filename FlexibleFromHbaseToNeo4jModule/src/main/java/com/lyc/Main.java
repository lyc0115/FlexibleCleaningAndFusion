package com.lyc;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @ProjectName FlexibleCleaningAndFusion
 * @ClassName Main
 * @Description TODO
 * @Author lyc
 * @Date 2021/8/7 14:37
 **/
public class Main {
    public static void main(String[] args) throws IOException {
        String filePath = args[0];

        ConfigInit.initIndexRule(filePath);
        //获取表属性关系建立参数
        ArrayList<String> tablerel = ConfigInit.indexRel;

        //初始化配置并建立与neo4j连接
        AboutHbase.wakeupHbase(filePath);

        //创建节点
        ArrayList<String> tables = ReadConfigFile.ReadConfigItem(filePath, "转化表名=");
        for (String table : tables) {
            AboutHbase.SetNeo4jNode(table);
        }

        System.out.println("*****************开始关系创建*****************");
        //创建关系
        for (String rel : tablerel) {
            String tableA = rel.split("<=>")[0];

            String indexA = rel.split("<=>")[1];

            String tableB = rel.split("<=>")[2];

            String module = rel.split("<=>")[3];

            String reltype = rel.split("<=>")[4];

            System.out.println("创建关系开始!!!!!!!!!!!!!!!!!!!!!!!");
            //创建关系
            AboutHbase.SetNeo4jRel(tableA, indexA, tableB, module, reltype);
        }
        AboutHbase.close();

    }
}
