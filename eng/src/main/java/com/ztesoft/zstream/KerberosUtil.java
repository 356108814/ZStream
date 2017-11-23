package com.ztesoft.zstream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedAction;

/**
 * kerberos工具类
 *
 * @author Yuri
 */
public class KerberosUtil {
    private static Logger logger = LoggerFactory.getLogger(KerberosUtil.class);
    private static UserGroupInformation ugi;
    private static boolean isUseKerberos = true;
    private static boolean isDebug = true;

    /**
     * 登陆集群
     *
     * @return boolean 是否登陆成功
     */
    public static boolean loginCluster(boolean isUseKerberos, boolean isDebug) throws IOException {
        KerberosUtil.isUseKerberos = isUseKerberos;
        KerberosUtil.isDebug = isDebug;
        boolean success = true;
        if (isUseKerberos) {
            if (isDebug) {
                success = debugAuth();
                System.out.println("===============Debug login:" + success + "===============");
            } else {
                UserGroupInformation ugi = auth();
                success = ugi != null;
                String result = success ? ugi.toString() : "null";
                System.out.println("===============login:" + result + "===============");
            }
        } else {
            System.out.println("===============not use kerberos login===============");
        }
        return success;
    }

    /**
     * 调试模式下验证
     *
     * @return true验证成功，false验证失败
     * @throws IOException
     */
    private static boolean debugAuth() throws IOException {
        boolean isWindows = System.getProperty("os.name").toLowerCase().startsWith("win");
        String keytabPath = "/home/keydir/bdp.keytab";
        if (isWindows) {
            keytabPath = "F:/bdp.keytab";
        }
        return auth("bdp/admin", keytabPath);
    }

    /**
     * kerberos登陆验证
     *
     * @param user       用户。如：bdp/admin
     * @param keytabPath keytab文件路径。如：F:/bdp.keytab
     * @return true，验证成功
     * @throws IOException
     */
    private static boolean auth(String user, String keytabPath) throws IOException {
        init();
        Configuration conf = createHbaseConfig();

        UserGroupInformation.setConfiguration(conf);
        ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(user, keytabPath);
        UserGroupInformation.setLoginUser(ugi);
        return ugi.hasKerberosCredentials();
    }

    /**
     * 根据当前机器ticket缓存验证
     *
     * @return UserGroupInformation
     * @throws IOException
     */
    private static UserGroupInformation auth() throws IOException {
        String principal = System.getProperty("sun.security.krb5.principal", System.getProperty("user.name"));
        UserGroupInformation.setConfiguration(createHdfsConfig());
        ugi = UserGroupInformation.getUGIFromTicketCache(null, principal);
        return ugi;
    }

    public static Configuration createHdfsConfig() {
        Configuration conf = new Configuration();
        conf.setClassLoader(KerberosUtil.class.getClassLoader());
        Path hdfsPath = new Path("hdfs-site.xml");
        conf.addResource(hdfsPath);

        Path coreSitePath = new Path("core-site.xml");
        conf.addResource(coreSitePath);
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        return conf;
    }

    public static Configuration createHbaseConfig() {
        Configuration conf = createHdfsConfig();
        Configuration hbaseConf = HBaseConfiguration.create(conf);
        hbaseConf.setClassLoader(KerberosUtil.class.getClassLoader());
        Path hbasePath = new Path("hbase-site.xml");
        hbaseConf.addResource(hbasePath);
        hbaseConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        logger.info("连接Hbase,hbase.zookeeper.quorum=" + hbaseConf.get("hbase.zookeeper.quorum"));

        return hbaseConf;
    }

    public static UserGroupInformation getUgi() {
        return ugi;
    }

    /**
     * 创建hbase连接
     *
     * @return Connection
     * @throws IOException
     */
    public static Connection createHbaseConnection() throws IOException {
        final Connection[] connection = {null};
        loginCluster(isUseKerberos, isDebug);
        final Configuration hbaseConf = createHbaseConfig();
        if (isUseKerberos) {
            ugi.doAs(new PrivilegedAction<Object>() {
                @Override
                public Object run() {
                    try {
                        connection[0] = ConnectionFactory.createConnection(hbaseConf);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return null;
                }
            });
        } else {
            connection[0] = ConnectionFactory.createConnection(hbaseConf);
        }
        return connection[0];
    }

    private static void init() {
        System.setProperty("java.security.krb5.kdc", "host66");
        System.setProperty("java.security.krb5.realm", "NBDP.COM");
    }
}
