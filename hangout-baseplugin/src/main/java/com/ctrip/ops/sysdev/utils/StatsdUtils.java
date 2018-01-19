package com.ctrip.ops.sysdev.utils;

import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;

/**
 * @author meng.fanyuan@puscene.com
 * @date 2018/1/17.
 */
public class StatsdUtils {
    private static final StatsDClient statsd = new NonBlockingStatsDClient("hangout", "127.0.0.1", 8125);

    public static StatsDClient getClient () {
        return statsd;
    }

}
