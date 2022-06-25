package com.lyc;

import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.ArrayList;

/**
 * @ProjectName FlexibleCleaningAndFusion
 * @ClassName Main
 * @Description TODO xml数据导入hbase模块主入口
 * @Author lyc
 * @Date 2021/7/9 21:05
 * @Version 1.0
 **/
public class Main {
    public static void main(String[] args) throws IOException, ParserConfigurationException, SAXException {
        //配置文件路径
        String filepath = args[0];

        //获取hbase的IP地址
        String hbaseip = ReadConfigFile.ReadConfigItem(filepath,"HbaseIP地址与端口=").get(0).split("<=>")[0];

        //获取hbase的端口号
        String hbaseport = ReadConfigFile.ReadConfigItem(filepath,"HbaseIP地址与端口=").get(0).split("<=>")[1];

        //获取xml文件夹路径
        String xmlpath = ReadConfigFile.ReadConfigItem(filepath,"XML文件夹路径=").get(0);

        //获取xml导入hbase的表名
        ArrayList<String> hbasetables = ReadConfigFile.ReadConfigItem(filepath,"xml导入hbase表名=");

        SetHbase sethbase = new SetHbase();
        XmlAnalysis xmlanalysis = new XmlAnalysis();
        xmlanalysis.conffilepath = filepath;
        SetHbase.conffilepath = filepath;
        //根据配置信息初始化hbase
        sethbase.initHbase(hbaseip,hbaseport);
        //通过给定文件夹路径，查找该文件夹下的所有文件及文件名
        String[] array = xmlanalysis.xmlFilePath(xmlpath);
        for (String a:array){
            for (String table:hbasetables){
                sethbase.fileToHbase(table,xmlanalysis.filexmlAnalysis(xmlpath+"/"+a,table));
            }
        }
        sethbase.close();

    }
}
