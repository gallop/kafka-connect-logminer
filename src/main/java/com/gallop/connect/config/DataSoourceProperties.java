package com.gallop.connect.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.transform.Source;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLOutput;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * author gallop
 * date 2021-09-08 9:56
 * Description:
 * Modified By:
 */
public class DataSoourceProperties {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataSoourceProperties.class);
    private static final String PROPERTIES_FILE = "logminer.source.properties";
    private static Map<String, String> properties;
    static {
        try {
            properties = new HashMap<>();
            //1、加载配置文件
            Properties pros = new Properties();
            //使用ClassLoader加载配置文件，获取字节输入流
            InputStream is = DataSoourceProperties.class.getClassLoader().getResourceAsStream(PROPERTIES_FILE);
            pros.load(is);

            for (String pro : pros.stringPropertyNames()) {
                String pointerValue = pros.getProperty(pro);
                //LOGGER.info("pointer:{},pointerValue:{}",pro,pointerValue);
                System.out.println("pointer:"+pro+", value:"+pointerValue);
                properties.put(pro,pointerValue);
            }
        } catch (IOException exception) {
            exception.printStackTrace();
        }
    }

    public static Map<String, String> getProperties() {
        return properties;
    }

    public static void main(String[] args) {
        //LOGGER.info("properties:{}",properties);
        System.out.println("properties:"+properties);
    }
}
