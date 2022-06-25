package com.lyc;

import java.io.IOException;

/**
 * @ProjectName FlexibleCleaningAndFusion
 * @ClassName Main
 * @Description TODO Hbase绑定ID模块
 * @Author lyc
 * @Date 2021/7/20 9:59
 **/
public class Main {
    public static void main(String[] args) throws IOException {
        //配置文件路径
        String filePath = args[0];
        //绑定id的habse表
        String tables = ReadConfigFile.ReadConfigItem(filePath, "绑定ID的hbase表名=").get(0);
        //获取区分标志符
        String tableFlag = ReadConfigFile.ReadConfigItem(filePath, "绑定ID的hbase表区分标识符=").get(0);

        String[] tableNames = tables.split("<=>");
        String[] tableFlags = tableFlag.split("<=>");
        for (int i = 0; i < tableNames.length; i++) {
            AboutHbase.wakeupHbase();
            System.out.println("开始处理表：" + tableNames[i]);
            AboutHbase.SetHbaseID(tableNames[i], tableFlags[i]);
            System.out.println("处理结束表：" + tableNames[i]);
            AboutHbase.close();
        }
    }
}
