package com.wycweb.bigdata.common.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;
import java.util.Properties;

public class HdfsUtils {

    private static LoadLocalResource config = new LoadLocalResource();
    private static Properties properties = config.getConfig("/env-conf.properties");
    private static final String HDFS_PATH = properties.getProperty("hadoop.fs.defaultFS");
    private static final String USER = properties.getProperty("code.runtime.hadoop.user");

    /**
     * 读取hdfs文件
     *
     * @param filePath hdfs文件路径
     * @return
     * @throws Exception
     */
    public static FSDataInputStream loadHdfsField(String filePath) throws Exception {

        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, USER);

        return fileSystem.open(new Path(filePath));
    }
}
