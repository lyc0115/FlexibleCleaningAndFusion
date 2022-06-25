package com.lyc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

/**
 * @ProjectName FlexibleCleaningAndFusion
 * @ClassName AboutHbase
 * @Description TODO
 * @Author lyc
 * @Date 2021/8/7 14:39
 **/
public class AboutHbase {

    //Hbase数据库连接对象
    public static Connection connection;

    //数据库元数据操作对象
    public static Admin admin;

    public static Session session;

    public static Driver driver;

    public static void wakeupHbase(String filepath) throws IOException {

        //获取hbase数据库配置对象
        Configuration conf = HBaseConfiguration.create();

        connection = ConnectionFactory.createConnection(conf);
        admin = connection.getAdmin();
        driver = GraphDatabase.driver(ConfigInit.url, AuthTokens.basic(ConfigInit.username, ConfigInit.passwd));
        session = Controller.wakeupSession(driver);
    }

    public static void close() throws IOException {
        admin.close();
        connection.close();
        Controller.closeSession(driver, session);
    }

    public static void SetNeo4jNode(String tableName) throws IOException {
        //获取hbase表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        //获取表中的数据
        ResultScanner scanner = table.getScanner(new Scan());

        //循环输出表中数据
        for (Result result : scanner) {
            List<Cell> listCells = result.listCells();

            //以键值对形式保存表中数据，方便neo4jsql语句处理
            HashMap<String, String> nodevalues = new HashMap<>();
            for (Cell cell : listCells) {
                //存储hbase中列限定符
                String key = Bytes.toString(CellUtil.cloneQualifier(cell));

                //存储hbase中列限定符对应的值
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                //替换大部分空白字符(\s 可以匹配空格、制表符、换页符等空白字符的其中任意一个)
                value = value.replaceAll("\\s*","");
//                String[] cm = {".", "\\[", "=~", "^", "\\*", "/", "%", "\\+", "-", "=", "~", "<>", "!=", "<", ">", "<=", ">=","}","\"","/","\'","\"","\\"};
//                for (int i = 0; i < cm.length; i++) {
//                    value = value.replace(cm[i],"");
//                }

                nodevalues.put(key, value);
            }

            for (String kv: ConfigInit.globalkeyvalue){
                String key_g = kv.split("<=>")[0];
                String value_g = kv.split("<=>")[1];
                if(value_g.equals("null")){
                    value_g = "";
                }
                nodevalues.put(key_g,value_g);
            }

            String nodetype = "ProjectX:" + tableName;
            Controller.CreateOrUpdateNode(session, nodetype, nodevalues);
        }

    }

    public static void SetNeo4jRel(String tablenameA, String indexA, String tablenameB, String module, String reltype) throws IOException {

        System.out.println("创建表" + tablenameA + "至表" + tablenameB + "关系" );
        String nodeAtype = "ProjectX:" + tablenameA;
        String nodeBtype = "ProjectX:" + tablenameB;

        //主表中的ID
        String nodeAtypeID = tablenameA + "ID";

        String comparevalue = "";

        //获取数据表对象
        Table table = connection.getTable(TableName.valueOf(tablenameA));

        //获取表中数据
        ResultScanner scanner = table.getScanner(new Scan());

        //循环输出表中的数据
        for (Result result : scanner) {
            List<Cell> cells = result.listCells();
            //主表中关系的id集合
            String idstr = "";
            //主表id
            String thisid = "";

            for (Cell cell : cells) {
                if (indexA.equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                    idstr = Bytes.toString(CellUtil.cloneValue(cell));
                }else if (nodeAtypeID.equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                    thisid = Bytes.toString(CellUtil.cloneValue(cell));
                }
            }

            String[] ids = idstr.split(";");
            if (module.equals("module1")) {
                for (String id : ids) {
                    Controller.CreateRel(session, nodeAtype, nodeBtype, thisid, id, module, reltype);
                }
            }else if (module.equals("module2")) {
                for (String id : ids) {
                    Controller.CreateRel(session, nodeAtype, nodeBtype, thisid, id, module, reltype);
                }
            }
        }

    }

}
