package com.gmg.demo;

/**
 * @author gmg
 * @title: BBossESStarterTestCase
 * @projectName esLearning
 * @description: TODO
 * @date 2020/3/10 17:02
 */
import org.frameworkset.elasticsearch.client.ImportBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;


@RunWith(SpringRunner.class)
@SpringBootTest
public class EsStarterTestCase {


    @Test
    public void testBbossESStarter() throws Exception {
        ImportBuilder importBuilder = ImportBuilder.newInstance();


    }

}
