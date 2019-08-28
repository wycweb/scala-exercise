package com.wycweb.bigdata.framework.common.utils;


import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 获取resource目录下的配置
 */
public class LoadLocalResource {
    public Properties getConfig(String pathName) {

        Properties properties = new Properties();
        InputStream inputStream = this.getClass().getResourceAsStream(pathName);

        try {
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return properties;
    }

}
