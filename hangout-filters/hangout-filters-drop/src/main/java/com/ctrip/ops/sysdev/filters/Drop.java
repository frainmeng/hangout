package com.ctrip.ops.sysdev.filters;

import com.ctrip.ops.sysdev.baseplugin.BaseFilter;
import com.ctrip.ops.sysdev.utils.StatsdUtils;

import java.util.Map;

public class Drop extends BaseFilter {
	public Drop(Map config) {
		super(config);
	}

    @Override
    protected Map filter(Map event) {
		StatsdUtils.getClient().increment("discard.count");
		return null;
	}
}
