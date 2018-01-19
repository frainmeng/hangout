package com.ctrip.ops.sysdev.outputs;

import java.util.List;
import java.util.Map;

import com.ctrip.ops.sysdev.baseplugin.BaseOutput;
import lombok.extern.log4j.Log4j;

@Log4j
public class Stdout extends BaseOutput {

    public Stdout(Map config) {
        super(config);
    }

    public Stdout(Map config, List<BaseOutput> errorOutputProcessors) {
        super(config,errorOutputProcessors);
    }

    @Override
    protected void prepare() {
    }

    @Override
    protected void emit(Map event) {
        System.out.println(event);
    }
}

