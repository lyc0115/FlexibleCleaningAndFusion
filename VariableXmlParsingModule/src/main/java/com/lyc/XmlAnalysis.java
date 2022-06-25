package com.lyc;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

/**
 * @ProjectName FlexibleCleaningAndFusion1.0
 * @ClassName XmlAnalysis
 * @Description TODO 解析xml数据
 * @Author lyc
 * @Date 2021/7/9 20:45
 * @Version 1.0
 **/
public class XmlAnalysis {

    /**
     *配置文件路径
     **/
    public String conffilepath = "";

    /**
     * @Description: TODO xml数据解析
     * @Author: lyc
     * @Date: 2021/7/9 20:50
     * @param filepath: xml数据存储路径
     * @param tablename: hbase表名
     * @return: com.lyc.Item
     **/
    public Item filexmlAnalysis(String filepath, String tablename) throws IOException, ParserConfigurationException, SAXException {
        //从配置文件中获取表属性标签
        ArrayList<String> xmlindex = ReadConfigFile.ReadConfigItem(conffilepath,tablename+"表xml标签属性=");
        Item item = new Item();
        //存储重复元素类中重复的个数
        int num = 0;
        System.out.println("创建DOM解析器工厂");
        //创建DOM解析器工厂
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        System.out.println("获取解析器对象");
        //获取解析器对象
        DocumentBuilder db = dbf.newDocumentBuilder();
        //调用DOM解析器对象paerse（string uri）方法得到Document对象
        Document doc = db.parse(filepath);
        //获得NodeList对象
        NodeList nl = doc.getElementsByTagName("Document");
        //遍历XML文件中的各个元素
        for (int i = 0; i < nl.getLength(); i++) {
            //得到Nodelist中的Node对象
            Node node = nl.item(i);
            //强制转化得到Element对象
            Element element = (Element) node;

            //获取各个元素的属性值
            System.out.println("开始传递数据");
            for (String xm:xmlindex){
                //getElementByTagName: 通过标签名获取此标签
                if(element.getElementsByTagName(xm).getLength() != 0){
                    //获取标签个数
                    num = element.getElementsByTagName(xm).getLength();
                    for (int j = 0; j < num; j++) {
                        //第一次直接存储，后面做拼接后存储，防止后续操作覆盖掉前面操作
                        if (j == 0) {
                            item.itemmap.put(xm,element.getElementsByTagName(xm).item(j).getTextContent());
                        }else {
                            String value = item.itemmap.get(xm) + ";"+element.getElementsByTagName(xm).item(j).getTextContent();
                            item.itemmap.put(xm,value);
                        }

                    }
                    System.out.println("传递成功  "+xm+ "("+item.itemmap.get(xm)+")" + element.getElementsByTagName(xm).item(0).getTextContent());
                }
            }
        }

        return item;
    }


    /**
     * @Description: TODO 通过给定文件夹路径，查找该文件夹下的所有文件及文件名，并存储至泛型中
     * @Author: lyc
     * @Date: 2021/7/9 21:15
     * @param xmlpath:  文件夹路径
     * @return: java.lang.String[]
     **/
    public String[] xmlFilePath(String xmlpath) {
        File file = new File(xmlpath);
        return file.list();
    }
}
