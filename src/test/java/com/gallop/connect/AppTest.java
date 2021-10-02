package com.gallop.connect;

import static org.junit.Assert.assertTrue;

import com.gallop.connect.config.DataSoourceProperties;
import com.gallop.connect.logminer.source.LogMinerSQLParser;
import net.sf.jsqlparser.JSQLParserException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Unit test for simple App.
 */
public class AppTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(AppTest.class);
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue() {
        assertTrue( true );
    }

    @Test
    public void getPropertiesTest() throws IOException {
        LOGGER.info("getProperties:"+ DataSoourceProperties.getProperties());
    }

    public static void main(String[] args) {
        String redoSql="insert into \"USERCENTER\".\"test_user\"(\"id\",\"name\",\"age\") values ('abc003','jetty','28');";
        try {
            Map<String, Map<String, String>> changes = LogMinerSQLParser.parseRedoSQL(redoSql);
            System.err.println("changes:"+changes);
            Map<String, String> before = changes.get("BEFORE");
            System.out.println("before:"+before);
        } catch (JSQLParserException e) {
            e.printStackTrace();
        }
    }
}
