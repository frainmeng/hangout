package com.ctrip.ops.sysdev.outputs;

import com.ctrip.ops.sysdev.baseplugin.BaseOutput;
import com.ctrip.ops.sysdev.render.FreeMarkerRender;
import com.ctrip.ops.sysdev.render.TemplateRender;
import lombok.extern.log4j.Log4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author meng.fanyuan@puscene.com
 * @date 2018/1/19.
 */
@Log4j
public class Log extends BaseOutput {

    private static final String FILE_PATH_FIELD = "filePath";
    private static final String TEMPLATE_FIELD = "template";

    private Logger outputLogger ;
    private String filePath;
    private TemplateRender templateRender;
    private boolean needRender;
    public Log(Map config) {
        super(config);
    }

    public Log(Map config, List<BaseOutput> errorOutputProcessors) {
        super(config,errorOutputProcessors);

    }

    @Override
    protected void prepare() {
        filePath = (String) config.get(FILE_PATH_FIELD);
        String template = (String) config.get(TEMPLATE_FIELD);
        if (StringUtils.isBlank(filePath)) {
            log.error("Log output plugin field 'filePath' can not blank");
        }
        if (StringUtils.isNotBlank(template)) {
            try {
                templateRender = new FreeMarkerRender(template,"logOutputRender");
                needRender = true;
            } catch (IOException e) {
                log.error("",e);
                System.exit(1);
            }
        }


        this.outputLogger = LogManager.getLogger(Log.class);
        outputLogger.removeAllAppenders();
        DailyRollingFileAppender appender = new DailyRollingFileAppender();
        appender.setAppend(true);
        PatternLayout patternLayout = new PatternLayout("%d%m%n");
        appender.setFile(filePath);
        appender.setLayout(patternLayout);
        appender.activateOptions();
        appender.setName("OutPutErrorAppender");
        appender.setThreshold(Level.INFO);
        outputLogger.setLevel(Level.INFO);
        outputLogger.addAppender(appender);
    }

    @Override
    protected void emit(Map event) {
        if (needRender) {
            String str = templateRender.render(event).toString();
            if (str == null) {
                log.error("log output plugin render template error");
            }
            outputLogger.info(str);
        } else {
            outputLogger.info(event);
        }
    }
}
