package com.lyc;

import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

/**
 * @ProjectName FlexibleCleaningAndFusion
 * @ClassName Controller
 * @Description TODO
 * @Author lyc
 * @Date 2021/8/7 14:47
 **/
public class Controller {

    /**
     * @Description: TODO 执行CSQL语句，向neo4j数据库中插入数据
     * @Author: lyc
     * @Date: 2021/8/7 15:13
     * @param session:
     * @param nodetype:
     * @param nodevalues:
     * @return: void
     **/
    public static void CreateOrUpdateNode(Session session, String nodetype, HashMap<String,String> nodevalues){

        String runstr = makeRunStr("createnode",nodevalues);

        //match (a:Projectx:table1{table1ID:"idvalue"}) return a;
        System.out.println("MATCH (a:"+nodetype+" {"+nodetype.split(":")[1]+"ID:\""+nodevalues.get(nodetype.split(":")[1]+"ID")+"\"}) RETURN a");
        //判断neo4j内是否存在该节点，存在则更新信息，不存在则创建信息
        StatementResult result = session.run( "MATCH (a:"+nodetype+" {"+nodetype.split(":")[1]+"ID:\""+nodevalues.get(nodetype.split(":")[1]+"ID")+"\"}) RETURN a");
//        StatementResult result = session.run( "MATCH (a:"+nodetype+" {id:\""+nodevalues.get("id")+"\"}) RETURN a");
        if (result.hasNext()){
            System.out.println("更新节点："+nodetype);
            UpdateNode(session,nodetype,nodevalues);
        }
        else {
            System.out.println("创建节点："+nodetype);
//            create (a:Projectx:table1Profile{k1:v1,k2:v2,...}) return a
            System.out.println("CREATE (a:"+nodetype+" {"+runstr+"}) RETURN a");
            session.run( "CREATE (a:"+nodetype+" {"+runstr+"}) RETURN a");
        }
    }

    /**
     * @Description:TODO 将Hbase中的存储的mysql主键，以属性名为”mysqlkey“形式存储在neo4j节点上
     * @Author: lyc
     * @Date: 2021/8/7 15:14
     * @param session:
     * @param nodetype:
     * @param htableidname:
     * @param hbasenodeid:
     * @param mysqlkey:
     * @return: void
     **/
    public static void CreateNodeMysqlkey(Session session,String nodetype,String htableidname,String hbasenodeid,String mysqlkey){
        if (nodetype == null || hbasenodeid == null || mysqlkey == null)
            return;
        System.out.println("match (n:"+ nodetype +") where n."+ htableidname +" = \""+ hbasenodeid +"\" set n.mysqlkey = \""+ mysqlkey +"\" return n");
        session.run("match (n:"+ nodetype +") where n."+ htableidname +" = \""+ hbasenodeid +"\" set n.mysqlkey = \""+ mysqlkey +"\" return n");
    }

    /**
     * @Description: TODO 两个节点创建关系
     * @Author: lyc
     * @Date: 2021/8/8 20:32
     * @param session:
     * @param nodeAtype:
     * @param nodeBtype:
     * @param nodeAid:
     * @param nodeBid:
     * @param linktype:
     * @return: void
     **/
    public static void CreateRel(Session session, String nodeAtype, String nodeBtype, String nodeAid, String nodeBid, String module, String linktype) {

        if (module.equals("module1")) {
            System.out.println("创建" + nodeAtype + "至" + nodeBtype + "关系---ID模式(module1)");
            //MATCH p=(a:ProjectX:table1 {table1ID:"table1id值"})-[]->(b:ProjectX:table2{table2ID:"table1中比较参数的保存对应副表的id值"}) RETURN p
            StatementResult result = session.run("MATCH p=(a:"+nodeAtype+" {"+nodeAtype.split(":")[1]+"ID:\""+nodeAid+"\"})-[]->(b:"+nodeBtype+"{"+nodeBtype.split(":")[1]+"ID:\""+nodeBid+"\"}) RETURN p");

            if (result.hasNext()) {
                return;
            }else {
                System.out.println("MATCH (a:"+nodeAtype+" {"+nodeAtype.split(":")[1]+"ID:\""+nodeAid+"\"}),(b:"+nodeBtype+" {"+nodeBtype.split(":")[1]+"ID:\""+nodeBid+"\"}) CREATE (a)-[r:"+linktype+"]->(b) RETURN r");
                session.run( "MATCH (a:"+nodeAtype+" {"+nodeAtype.split(":")[1]+"ID:\""+nodeAid+"\"}),(b:"+nodeBtype+" {"+nodeBtype.split(":")[1]+"ID:\""+nodeBid+"\"}) CREATE (a)-[r:"+linktype+"]->(b) RETURN r");
            }
        }else if (module.equals("module2")) {
            System.out.println("创建" + nodeBtype + "至" + nodeAtype + "关系---ID模式(module2)");
            //StatementResult result = session.run("MATCH p=(a:"+nodeBtype+" {"+nodeBtype.split(":")[1]+"ID:\""+nodeBid+"\"})-[]->(b:"+nodeAtype+"{"+nodeAtype.split(":")[1]+"ID:\""+nodeAid+"\"}) RETURN p");
           //match p=(a:ProjectX:newProjectContract{id:"ee44e173a34c40c7806bf0ee30764ea7"})-[]->(b:ProjectX:Person{PersonID:"person1068779"}) return p
            StatementResult result = session.run("MATCH p=(a:"+nodeBtype+" {"+"id:\""+nodeBid+"\"})-[]->(b:"+nodeAtype+"{"+nodeAtype.split(":")[1]+"ID:\""+nodeAid+"\"}) RETURN p");
            if (result.hasNext()) {
                return;
            }else {
                System.out.println("MATCH (a:"+nodeBtype+" {"+nodeBtype.split(":")[1]+"ID:\""+nodeBid+"\"}),(b:"+nodeAtype+" {"+nodeAtype.split(":")[1]+"ID:\""+nodeAid+"\"}) CREATE (a)-[r:"+linktype+"]->(b) RETURN r");
                session.run("MATCH (a:"+nodeBtype+" {"+nodeBtype.split(":")[1]+"ID:\""+nodeBid+"\"}),(b:"+nodeAtype+" {"+nodeAtype.split(":")[1]+"ID:\""+nodeAid+"\"}) CREATE (a)-[r:"+linktype+"]->(b) RETURN r");
                System.out.println("MATCH (a:"+nodeBtype+" {"+"id:\""+nodeBid+"\"}),(b:"+nodeAtype+" {"+nodeAtype.split(":")[1]+"ID:\""+nodeAid+"\"}) CREATE (a)-[r:"+linktype+"]->(b) RETURN r");
                session.run("MATCH (a:"+nodeBtype+" {"+"id:\""+nodeBid+"\"}),(b:"+nodeAtype+" {"+nodeAtype.split(":")[1]+"ID:\""+nodeAid+"\"}) CREATE (a)-[r:"+linktype+"]->(b) RETURN r");
                //match (a:ProjectX:newProjectContract{id:"ee44e173a34c40c7806bf0ee30764ea7"}),(b:ProjectX:Person{PersonID:"person1068779"}) create (a)-[r:participantperson]->(b) return r
            }
        }

    }


    /**
     * @Description:TODO 更新node属性信息
     * @Author: lyc
     * @Date: 2021/8/7 15:19
     * @param session:
     * @param nodetype:
     * @param nodevalues:
     * @return: void
     **/
    public static void UpdateNode(Session session, String nodetype,HashMap<String,String> nodevalues){
        String runstr = makeRunStr("updatenode",nodevalues);
        session.run("MATCH (n:"+nodetype+" {"+nodetype.split(":")[1]+"ID: \"\'"+nodevalues.get(nodetype.split(":")[1]+"ID")+"\'\" })" + " SET " + runstr);
    }


    /**
     * @Description:TODO 按照属性与值，查询节点是否存在
     * @Author: lyc
     * @Date: 2021/8/7 15:19
     * @param session:
     * @param nodetype:
     * @param nodekey:
     * @param nodevalue:
     * @param comparekey:
     * @param comparevalue:
     * @return: java.lang.Boolean
     **/
    public static Boolean FindNode(Session session, String nodetype,String nodekey,String nodevalue,String comparekey,String comparevalue){
        StatementResult result;
        //判断neo4j内是否存在该节点，
        if(comparekey.equals("null") == true){
            result = session.run( "MATCH (a:"+nodetype+" {"+nodekey+":\""+nodevalue+"\"}) RETURN a");
        }
        else {
            result = session.run( "MATCH (a:"+nodetype+" {"+nodekey+":\""+nodevalue+"\","+comparekey+":\""+comparevalue+"\"}) RETURN a");
        }

        if (result.hasNext()){
            return true;
        }
        else {
            return false;
        }
    }


    /**
     * @Description:TODO 将字典中属性与值拼接为字符串
     * @Author: lyc
     * @Date: 2021/8/7 15:20
     * @param index:
     * @param nn:
     * @return: java.lang.String
     **/
    public static String makeRunStr(String index, HashMap<String,String> nn){
        String runs = "";
        Set<String> set = nn.keySet();
        Iterator<String> iterable = set.iterator();
        while (iterable.hasNext()){
            String key = iterable.next();
            String value = nn.get(key);
            if (index.equals("createnode")){
                runs += key +":\""+value+"\",";
            }
            else if (index.equals("updatenode")){
                runs += "n."+key+"=\""+value+"\",";
            }
        }
        runs = runs.substring(0,runs.length() - 1);
        return  runs;
    }

    /**
     * @Description:TODO 建立Session连接
     * @Author: lyc
     * @Date: 2021/8/7 15:21
     * @param driver:
     * @return: org.neo4j.driver.v1.Session
     **/
    public static Session wakeupSession(Driver driver) {
        Session session = driver.session();
        return session;

    }

    /**
     * @Description:TODO 关闭driver和session连接
     * @Author: lyc
     * @Date: 2021/8/7 15:21
     * @param driver:
     * @param session:
     * @return: void
     **/
    public static void closeSession(Driver driver,Session session){
        session.close();
        driver.close();
    }
}
