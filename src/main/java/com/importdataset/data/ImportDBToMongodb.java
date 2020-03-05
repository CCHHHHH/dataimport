package com.importdataset.data;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.importdataset.entity.DataSource;
import com.importdataset.entity.Label;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.log4j.LogManager;
import org.apache.spark.sql.*;
import org.bson.Document;

import java.sql.*;
import java.util.*;
import org.apache.log4j.Logger;

/**
 * 作者：chenhao
 * 日期：2020-02-28 14:17
 **/
public class ImportDBToMongodb {

    private static String URL = "jdbc:mysql://139.219.10.153:3306/clouddata";
    private static String USER = "root";
    private static String PASSWORD = "cloudlinx.2020";
    private static String MYSQLDRIVER = "com.mysql.jdbc.Driver";
    private static String ORACLEDRIVER = "oracle.jdbc.driver.OracleDriver";
    private static String SQLSERVERDRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    private static String ORACLE = "ORACLE";
    private static String MYSQL = "MYSQL";
    private static String SQLSERVER = "SQLSERVER";
    private static String MONGODBURL = "mongodb://139.219.10.153:8987/test";
    private static final Logger log = LogManager.getLogger(ImportDBToMongodb.class);

    private static SparkSession spark = null;

    static {
        spark = SparkSession
                .builder()
//                .master("local[*]")
                .master("spark://139.219.10.153:7077")
                .getOrCreate();
    }

    public static void main(String[] args) {
//        readDB(spark, "1583378114236"); //ORACLE
//        readDB(spark, "1583066137754"); //MYSQL
//        readDB(spark, "1583067805998"); //SQLSERVER
        JSONObject rule = JSONObject.parseObject(args[1]);
        readDB(spark, args[0], rule);
    }

    private static void readDB(SparkSession sparkSession, String dsabelid, JSONObject rule) {

        //读取表名和标签
        //finishTime startTime
        Long finishTime = rule.getLong("finishTime")/1000;
//        Long finishTime = 1583226047L;
        Long startTime = rule.getLong("startTime")/1000;
//        Long startTime = 1425373200L;


        //创建mysql的conn连接
        Connection conn = null;
        Statement statement = null;
        DataSource dataSource = new DataSource();
        List<Label> labels = new ArrayList<>();
        try {
            Class.forName(MYSQLDRIVER);
            conn = DriverManager.getConnection(URL, USER, PASSWORD);
            statement = conn.createStatement();


            //获取数据集表中数据源id
            String sql_1 = "select datasourcesid,dsabelid from cda_datasetmanage where dsabelid = " + dsabelid;
            ResultSet resultSet_1 = statement.executeQuery(sql_1);
            String datasourceid = null;
            while (resultSet_1.next()) {
                datasourceid = resultSet_1.getString("datasourcesid");
            }


            //读取数据源配置信息
            String sql_2 = "select * from cda_datasourcemanage where id = " + datasourceid;
            ResultSet resultSet_2 = statement.executeQuery(sql_2);
            while (resultSet_2.next()) {
                dataSource.setDsexample(resultSet_2.getString("dsexample"));
                dataSource.setDsip(resultSet_2.getString("dsip"));
                dataSource.setDsloginid(resultSet_2.getString("dsloginid"));
                dataSource.setDsname(resultSet_2.getString("dsname"));
                dataSource.setDsport(resultSet_2.getString("dsport"));
                dataSource.setDspwd(resultSet_2.getString("dspwd"));
                dataSource.setDstype(resultSet_2.getString("dstype"));
            }
            System.out.println(dataSource);


            //读取标签数据
            String sql_3 = "select * from cda_datasetlabel where dsabelid = " + dsabelid;
            ResultSet resultSet_3 = statement.executeQuery(sql_3);
            while (resultSet_3.next()) {
                Label label = new Label();
                label.setTablename(resultSet_3.getString("tablename"));
                label.setLabelid(resultSet_3.getString("labelid"));
                label.setLabelname(resultSet_3.getString("labelname"));
                label.setNewid(resultSet_3.getString("newid"));
                label.setNewname(resultSet_3.getString("newname"));
                label.setTimestampif(resultSet_3.getInt("timestampif"));
                labels.add(label);
            }

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (statement != null) {
                    statement.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }


        //拼接sql
        String dstype = dataSource.getDstype();
        StringBuilder label_sb = new StringBuilder();
        StringBuilder table_sb = new StringBuilder();
        StringBuilder oraclelabel_sb = new StringBuilder();
        StringBuilder oracletable_sb = new StringBuilder();
        HashSet<String> hashSet = new HashSet<>();
        HashSet<String> oracleset = new HashSet<>();
        String timestamp = "";
        StringBuilder nvl_sb = new StringBuilder();
        for (Label label : labels) {
            System.out.println(label.getLabelid());
            String[] split = label.getLabelid().split("\\.");
            //oraclesql拼接字段
            oraclelabel_sb.append("\"").append(label.getTablename()).append("\"").append(".").append("\"").append(split[split.length - 1]).append("\"").append(",");
            oracleset.add("\"" + label.getTablename() + "\"");
            label_sb.append(label.getTablename()).append(".").append(split[split.length - 1]).append(",");
            hashSet.add(label.getTablename());
            if (!label.getTimestampif().equals(1)) {
                nvl_sb.append("nvl(" + split[split.length - 1] + ",\"\"),\",\",");
            } else {
                timestamp = split[split.length - 1];
                System.out.println(timestamp);
            }
        }
        for (String s : hashSet) {
            table_sb.append(s).append(",");
        }
        for (String s : oracleset) {
            oracletable_sb.append(s).append(",");
        }


        String label_str = label_sb.toString();
        String table_str = table_sb.toString();
        String sql_data = "select " + label_str.substring(0, label_str.length() - 1) + " from " + table_str.substring(0, table_str.length() - 1) + " where " + timestamp + "<=" + finishTime + " and " + timestamp + ">=" + startTime;
        String oracle_sql_data = "select " + oraclelabel_sb.substring(0, oraclelabel_sb.length() - 1) + " from " + oracletable_sb.substring(0, oracletable_sb.length() - 1) + " where " + "\"" + timestamp + "\"" + "<=" + finishTime + " and " + "\"" + timestamp + "\"" + ">=" + startTime;
        //TODO log
        System.out.println(oracle_sql_data);

        //数据结构
        StringBuilder struct_tag = new StringBuilder();
        ArrayList<JSONObject> documents = new ArrayList<>();
        JSONObject column = null;
        int i = 0;
        for (Label label : labels) {
            column = new JSONObject();
            struct_tag.append(label.getLabelid().split("\\.")[2]).append(",");
            column.put("colname", label.getLabelid());
            column.put("datatype", "DoubleType");
            column.put("orderby", i++);
            documents.add(column);
        }
        System.out.println("tablename:" + dsabelid);
        System.out.println("column:" + documents);
        String tagNames = struct_tag.toString().substring(0, struct_tag.toString().length() - 1);
        System.out.println("tagNames:" + tagNames);
        Document struct = new Document();
        struct.append("tablename", dsabelid).append("columns", documents.toString()).append("tagNames", tagNames);
        //导入数据结构到mongodb
        importDataStructMongoDB(struct);


        if (ORACLE.equals(dstype)) {
            System.out.println(sql_data);
            String temp = "temp" + System.currentTimeMillis();
            //从oracle中读取
            Dataset<Row> rowDataset = sparkSession.read().format("jdbc")
                    .option("url", "jdbc:oracle:thin:@//" + dataSource.getDsip() + ":" + dataSource.getDsport() + "/" + dataSource.getDsexample())
                    .option("driver", ORACLEDRIVER)
//                    .option("dbtable", "(select * from " + "\"" + table_str.substring(0, table_str.length() - 1) + "\"" + ")" + " temp")
                    .option("dbtable", "(" + oracle_sql_data + ") " + temp)
                    .option("user", dataSource.getDsloginid())
                    .option("password", dataSource.getDspwd()).load();

            System.out.println("=====oracle=====");
            rowDataset.show();

            //导入mongodb
            String sql = "select CAST(" + timestamp + " AS Long)  as `timestamp`,CONCAT(" + nvl_sb.toString().substring(0, nvl_sb.toString().length() - 5) + ") as data from " + temp;
            System.out.println(sql);
            Dataset<Row> dataset = dataRepeat(temp, sql, dsabelid, rowDataset);
            dataset.schema();
            dataset.show();


        } else if (MYSQL.equals(dstype)) {
            //从mysql中读取数据
            System.out.println(sql_data);
            String temp = "temp" + System.currentTimeMillis();
            //从数据源读取数据
            Dataset<Row> rowDataset = sparkSession.read().format("jdbc")
                    .option("url", "jdbc:mysql://" + dataSource.getDsip() + ":" + dataSource.getDsport() + "/" + dataSource.getDsexample())
                    .option("driver", MYSQLDRIVER)
                    .option("dbtable", "(" + sql_data + ")" + "as " + temp)
//                    .option("dbtable", "(select * from cda_datasetlabel) as temp")
                    .option("user", dataSource.getDsloginid())
                    .option("password", dataSource.getDspwd()).load();

            System.out.println("=====mysql=====");
            rowDataset.show();

            String sql = "select CAST(" + timestamp + " AS Long)  as `timestamp`,CONCAT(" + nvl_sb.toString().substring(0, nvl_sb.toString().length() - 5) + ") as data from " + temp;

            //导入mongodb
            Dataset<Row> dataset = dataRepeat(temp, sql, dsabelid, rowDataset);
            dataset.show();


        } else if (SQLSERVER.equals(dstype)) {
            System.out.println(sql_data);
            String temp = "temp" + System.currentTimeMillis();
            //从sqlserver中读取数据
            Dataset<Row> rowDataset = sparkSession.read().format("jdbc")
                    .option("url", "jdbc:sqlserver://" + dataSource.getDsip() + ":" + dataSource.getDsport() + ";DatabaseName=" + dataSource.getDsexample())
                    .option("driver", SQLSERVERDRIVER)
                    .option("dbtable", "(" + sql_data + ") " + temp)
//                    .option("dbtable", "(" + "select guest.city.cname,guest.city.cplace,guest.city.chistory from guest.city" + ")" + " temp")
                    .option("user", dataSource.getDsloginid())
                    .option("password", dataSource.getDspwd()).load();

            System.out.println("=====sqlserver=====");
            rowDataset.show();

            //导入数据到mongodb
            String sql = "select CAST(" + timestamp + " AS Long)  as `timestamp`,CONCAT(" + nvl_sb.toString().substring(0, nvl_sb.toString().length() - 5) + ") as data from " + temp;

            Dataset<Row> dataset = dataRepeat(temp, sql, dsabelid, rowDataset);
            dataset.show();
        }

    }

    private static void importMongoDB(Dataset<Row> ds, String dsabelid) {
        //导入数据到mongodb
        Map<String, String> map = new HashMap<>();
        map.put("spark.mongodb.output.uri", MONGODBURL);
        map.put("spark.mongodb.output.collection", dsabelid);

        ds.write()
                .options(map)
                .mode("overwrite")
                .format("com.mongodb.spark.sql")
                .save();
    }

    private static void importDataStructMongoDB(Document document) {
        //导入数据结构到mongodb
        //连接到 mongodb 服务
        MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
        //超时时间为5秒
        builder.serverSelectionTimeout(5000);
        MongoClientOptions build = builder.build();
        ServerAddress serverAddress = new ServerAddress("139.219.10.153", 8987);
        MongoClient mongoClient = new MongoClient(serverAddress, build);
        MongoDatabase test = mongoClient.getDatabase("test");
        MongoCollection<Document> schemaTable = test.getCollection("schemaTable");

        schemaTable.insertOne(document);
    }

    //数据格式转换
    private static Dataset<Row> dataRepeat(String tempTableName, String sql, String dsabelid, Dataset<Row> ds) {
        try {
            ds.createTempView(tempTableName);
            Dataset<Row> dataset = spark.sql(sql);
            //导入mongodb
            importMongoDB(dataset, dsabelid);
            return dataset;
        } catch (AnalysisException e) {
            throw new RuntimeException("数据转换错误", e);
        }
    }

}
