package QLI

import groovy.json.JsonSlurper
import org.apache.knox.gateway.shell.KnoxSession
import org.apache.knox.gateway.shell.hdfs.Hdfs
import org.apache.knox.gateway.shell.Credentials


import groovy.json.JsonSlurper


class Knox {


//    public Knox() {
//        gateway = "https://gateway-site:8443/gateway/secure"
//        session = KnoxSession.kerberosLogin(gateway)
//        text = Hdfs.ls( session ).dir( "/" ).now().string
//        json = (new JsonSlurper()).parseText( text )
//        println json.FileStatuses.FileStatus.pathSuffix
//        session.shutdown()
//    }

    String lol() {
        String gateway = "https://ip-10-11-21-224.alationdata.com:8443/gateway/default"
        KnoxSession session = KnoxSession.loginInsecure(gateway, "mduser", "hyp3rbAd").
        String text = Hdfs.ls( session ).dir( "/" ).now().string
        String ret = (new JsonSlurper()).parseText( text ).FileStatuses.FileStatus.pathSuffix
        session.shutdown()
        return ret
    }

}
