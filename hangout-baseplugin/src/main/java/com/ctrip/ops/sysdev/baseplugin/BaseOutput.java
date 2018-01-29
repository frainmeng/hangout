package com.ctrip.ops.sysdev.baseplugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.ctrip.ops.sysdev.utils.StatsdUtils;
import lombok.extern.log4j.Log4j;
import com.ctrip.ops.sysdev.render.FreeMarkerRender;
import com.ctrip.ops.sysdev.render.TemplateRender;
import org.apache.commons.collections4.CollectionUtils;

@Log4j
public abstract class BaseOutput extends Base {
    protected Map config;
    protected List<TemplateRender> IF;

    protected List<BaseOutput> errorOutputProcessors;


    public BaseOutput(Map config) {
        super(config);

        this.config = config;

        if (this.config.containsKey("if")) {
            IF = new ArrayList<TemplateRender>();
            for (String c : (List<String>) this.config.get("if")) {
                try {
                    IF.add(new FreeMarkerRender(c, c));
                } catch (IOException e) {
                    log.fatal(e.getMessage());
                    System.exit(1);
                }
            }
        } else {
            IF = null;
        }

        this.prepare();
    }

    public BaseOutput(Map config,List<BaseOutput> errorOutputProcessors) {
        this(config);
        this.errorOutputProcessors = errorOutputProcessors;
    }

    protected abstract void prepare();

    protected abstract void emit(Map event);

    public void shutdown() {
        log.info("shutdown" + this.getClass().getName());
    }

    public void process(Map event) {
        boolean ifSuccess = true;
        if (this.IF != null) {
            for (TemplateRender render : this.IF) {
                if (!render.render(event).equals("true")) {
                    ifSuccess = false;
                    break;
                }
            }
        }
        if (ifSuccess) {
            this.emit(event);
            if (this.enableMeter == true) {
                this.meter.mark();
            }
        }
    }

    /**
     * 错误时间
     * @param events 时间列表
     */
    public void processError (List<Map<String, Object>> events) {
        if (CollectionUtils.isNotEmpty(this.errorOutputProcessors)
                && CollectionUtils.isNotEmpty(events)) {

            errorOutputProcessors.forEach(baseOutput -> events.forEach(event -> {
                try {
                    StatsdUtils.getClient().increment("business."+event.get("business")+".error.count");
                    baseOutput.process(event);
                } catch (Exception e) {
                    log.error("错误处理输出异常",e);
                }
            }));
        }
    }
}
