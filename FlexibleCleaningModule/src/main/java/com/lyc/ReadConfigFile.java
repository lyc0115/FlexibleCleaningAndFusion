package com.lyc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

/**
 * @ProjectName FlexibleCleaningAndFusion
 * @ClassName ReadConfigFile
 * @Description TODO 配置类
 * @Author lyc
 * @Date 2021/7/10 9:50
 * @Version 1.0
 **/
public class ReadConfigFile {

    public static ArrayList<String> ReadConfigItem(String filePath, String itemName) throws IOException {
        ArrayList<String> arrayList = new ArrayList();
        //创建读取文本字符流
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream fsr = fs.open(new Path(filePath));
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fsr));
        //行对象
        String line = "";
        //保存需要的数据
        //循环遍历每行内容，截取需要的数据
        while((line = bufferedReader.readLine())!=null)
        {
            if(line.indexOf(itemName)>-1 && !line.startsWith("##")) {
                arrayList.add(line.substring(line.indexOf(itemName)+itemName.length()));
            }
        }
        if (arrayList.isEmpty()){
            System.out.println("配置文件中没有配置参数: "+ itemName);
        }
        bufferedReader.close();
        fsr.close();
        fs.close();
        return arrayList;
    }
}

