package MDE;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import QLI.Knox;
import kerberos.Kerberos;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.knox.gateway.shell.Credentials;

public class KnoxMeCaptain {

    public static final String zookeeperString = "hive.server2.authentication=KERBEROS;hive.server2.transport.mode=http;hive.server2.thrift.http.path=cliservice;hive.server2.thrift.http.port=10001;hive.server2.thrift.bind.host=ip-10-11-21-187.alationdata.com;hive.server2.use.SSL=false;hive.server2.authentication.kerberos.principal=hive/_HOST@TEST.ALATION.TEST";

    public static void doIt() throws Exception {
//        Configuration conf = KnoxMeCaptain.newConf(configs("/Users/chris.henderson/hack/Hive_MDE/matrix/jakes"));
//
//        Connection connection = null;
//        Statement statement = null;
//        ResultSet resultSet = null;
//
//            String gatewayHost = "ip-10-11-21-224.alationdata.com";
//            String gatewayPort = conf.get(Knox.GATEWAY_PORT);
//            String trustStore = "/Library/Java/JavaVirtualMachines/jdk-9.0.4.jdk/Contents/Home/lib/security/cacerts";
//            String trustStorePassword = "changeit";
//            String contextPath = "/gateway/default/hive";
////            String principal = conf.get(HiveConf.ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL.toString());
//        String principal = "HTTP/_HOST@TEST.ALATION.TEST";
////            String connectionString = String.format("jdbc:hive2://%s:%s;ssl=true;sslTrustStore=%s;trustStorePassword=%s?hive.server2.transport.mode=http;hive.server2.thrift.http.path=%s;principal=%s", gatewayHost, gatewayPort, trustStore, trustStorePassword, contextPath, principal);
//
//        String connectionString = "jdbc:hive2://ip-10-11-21-224.alationdata.com:8443/;ssl=true;principal=HTTP/ip-10-11-21-224.alationdata.com@TEST.ALATION.TEST;transportMode=http;httpPath=gateway/default/hive;sslTrustStore=/Library/Java/JavaVirtualMachines/jdk-9.0.4.jdk/Contents/Home/lib/security/cacerts;trustStorePassword=changeit";
////            Credentials credentials = new Credentials();
////            credentials.add("ClearInput", "Enter username: ", "mduser")
////                    .add("HiddenInput", "Enter pas" + "sword: ", "hyp3rbAd");
////            credentials.collect();
////
////            String username = credentials.get("user").string();
////            String password = credentials.get("pass").string();
//
//        String username = "mduser";
//        String password = "hyp3rbAd";
//            // Load Hive JDBC Driver
//            Class.forName( "org.apache.hive.jdbc.HiveDriver" );
//
//            // Configure JDBC connection
//            connection = DriverManager.getConnection(connectionString);
////
//            statement = connection.createStatement();
////
//            statement.execute("SHOW DATABASES");
//            System.out.println(statement.getResultSet());
//            System.out.println("DERP");
//            // Disable Hive authorization - This can be ommited if Hive authorization is configured properly
//            statement.execute( "set hive.security.authorization.enabled=false" );
//
//            // Drop sample table to ensure repeatability
//            statement.execute( "DROP TABLE logs" );
//
//            // Create sample table
//            statement.execute( "CREATE TABLE logs(column1 string, column2 string, column3 string, column4 string, column5 string, column6 string, column7 string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '" );
//
//            // Load data into Hive from file /tmp/log.txt which is placed on the local file system
//            statement.execute( "LOAD DATA LOCAL INPATH '/tmp/sample.log' OVERWRITE INTO TABLE logs" );
//
//            resultSet = statement.executeQuery( "SELECT * FROM logs" );
//
//            while ( resultSet.next() ) {
//                System.out.println( resultSet.getString( 1 ) + " --- " + resultSet.getString( 2 ) );
//            }
//        } catch ( ClassNotFoundException ex ) {
//            Logger.getLogger( HiveJDBCSample.class.getName() ).log( Level.SEVERE, null, ex );
//        } catch ( SQLException ex ) {
//            Logger.getLogger( HiveJDBCSample.class.getName() ).log( Level.SEVERE, null, ex );
//        } finally {
//            if ( resultSet != null ) {
//                try {
//                    resultSet.close();
//                } catch ( SQLException ex ) {
//                    Logger.getLogger( HiveJDBCSample.class.getName() ).log( Level.SEVERE, null, ex );
//                }
//            }
//            if ( statement != null ) {
//                try {
//                    statement.close();
//                } catch ( SQLException ex ) {
//                    Logger.getLogger( HiveJDBCSample.class.getName() ).log( Level.SEVERE, null, ex );
//                }
//            }
//            if ( connection != null ) {
//                try {
//                    connection.close();
//                } catch ( SQLException ex ) {
//                    Logger.getLogger( HiveJDBCSample.class.getName() ).log( Level.SEVERE, null, ex );
//                }
//            }
//        }
//    }
        }


    public static InputStream[] configs(String dir) {
        ArrayList<InputStream> streams = new ArrayList<>();
        buildConfigs(streams, dir);
        return streams.toArray(new InputStream[]{});
    }

    public static void buildConfigs(ArrayList<InputStream> streams, String dir) {
        File directory = new File(dir);
        for (File file : directory.listFiles()) {
            if (file.isDirectory()) {
                buildConfigs(streams, file.getAbsolutePath());
                continue;
            }
            if (file.getName().endsWith(".xml")) {
                try {
                    streams.add(new FileInputStream(file));
                    continue;
                } catch (IOException e) {
                    System.out.println("failed to open the config at " + file.getAbsolutePath());
                    System.out.println(e.getMessage());
                    System.exit(1);
                }
            }
            if (Kerberos.KRB5_CONF.equals(file.getName())) {
                Kerberos.setKrb5Conf(file.getAbsolutePath());
            }
        }
    }

    public static HiveConf newConf(InputStream... configurations) {
        HiveConf conf = new HiveConf();
        for (InputStream configuration : configurations) {
            conf.addResource(configuration);
        }
        return conf;
    }
}
