package com.alibaba.profiler.util;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;

/**
 * @author wxy.
 */
public class IpUtil {

    private static String cacheLocalIp;

    public static String getLocalIp() {

        if (cacheLocalIp != null) {
            return cacheLocalIp;
        }
        String localIp = null;
        try {

            Enumeration<NetworkInterface> netInterfaces =
                NetworkInterface.getNetworkInterfaces();
            InetAddress ip;
            boolean finded = false;

            while (netInterfaces.hasMoreElements() && !finded) {
                NetworkInterface ni = netInterfaces.nextElement();
                Enumeration<InetAddress> address = ni.getInetAddresses();
                while (address.hasMoreElements()) {
                    ip = address.nextElement();
                    if (!ip.isSiteLocalAddress()
                        && !ip.isLoopbackAddress()
                        && !ip.getHostAddress().contains(":")) {
                        finded = true;
                        break;
                    } else if (ip.isSiteLocalAddress()

                        && !ip.isLoopbackAddress()

                        && !ip.getHostAddress().contains(":")) {
                        localIp = ip.getHostAddress();

                    }
                }
            }

        } catch (Exception e) {
            // Do Nothing
        }
        cacheLocalIp = localIp;
        return localIp;
    }
}
