package com.gmg.demo;

/**
 * @author gmg
 * @title: BBossESStarterTestCase
 * @projectName esLearning
 * @description: TODO
 * @date 2020/3/10 17:02
 */
import org.frameworkset.elasticsearch.ElasticSearchHelper;
import org.frameworkset.tran.DataStream;
import org.frameworkset.tran.db.input.es.DB2ESImportBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

@RunWith(SpringRunner.class)
@SpringBootTest
public class EsStarterTestCase {


    @Test
    public void testSimpleImportBuilder(){
        DB2ESImportBuilder importBuilder = DB2ESImportBuilder.newInstance();
        try {
            //清除测试表数据
            ElasticSearchHelper.getRestClientUtil().dropIndice("posts");
        }
        catch (Exception e){

        }
        //数据源相关配置，可选项，可以在外部启动数据源
        importBuilder.setDbName("blog")
                .setDbDriver("com.mysql.jdbc.Driver") //数据库驱动程序，必须导入相关数据库的驱动jar包
                .setDbUrl("jdbc:mysql://localhost:3306/blog?useCursorFetch=true") //通过useCursorFetch=true启用mysql的游标fetch机制，否则会有严重的性能隐患，useCursorFetch必须和jdbcFetchSize参数配合使用，否则不会生效
                .setDbUser("root")
                .setDbPassword("root")
                .setValidateSQL("select 1")
                .setUsePool(false);//是否使用连接池


        //指定导入数据的sql语句，必填项，可以设置自己的提取逻辑
        importBuilder.setSql("select * from posts");
        /**
         * es相关配置
         */
        importBuilder
                .setIndex("posts") //必填项
                .setIndexType("doc") //es 7以后的版本不需要设置indexType，es7以前的版本必需设置indexType
                .setRefreshOption(null)//可选项，null表示不实时刷新，importBuilder.setRefreshOption("refresh");表示实时刷新
                .setUseJavaName(true) //可选项,将数据库字段名称转换为java驼峰规范的名称，例如:doc_id -> docId
                .setBatchSize(5000)  //可选项,批量导入es的记录数，默认为-1，逐条处理，> 0时批量处理
                .setJdbcFetchSize(10000);//设置数据库的查询fetchsize，同时在mysql url上设置useCursorFetch=true启用mysql的游标fetch机制，否则会有严重的性能隐患，jdbcFetchSize必须和useCursorFetch参数配合使用，否则不会生效


        importBuilder.addFieldMapping("name","name")
                .addFieldMapping("description","description")
                .addFieldMapping("studymodel","studymodel")
                .addFieldMapping("price","price")
                .addFieldMapping("pic","pic")
                .addFieldMapping("timestamp","timestamp");
        /**
         * 执行数据库表数据导入es操作
         */
        DataStream dataStream = importBuilder.builder();
        dataStream.execute();
    }

    @Test
    public  void testDb() throws ClassNotFoundException, SQLException {

        //1.注册数据库的驱动
        Class.forName("com.mysql.jdbc.Driver");
        //2.获取数据库连接（里面内容依次是："jdbc:mysql://主机名:端口号/数据库名","用户名","登录密码"）
        Connection
                connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/blog","root","root");

        System.out.println(connection.getClientInfo());
    }

}
