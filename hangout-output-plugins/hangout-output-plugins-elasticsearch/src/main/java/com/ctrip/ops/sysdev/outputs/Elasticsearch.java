package com.ctrip.ops.sysdev.outputs;

import com.ctrip.ops.sysdev.baseplugin.BaseOutput;
import com.ctrip.ops.sysdev.render.DateFormatter;
import com.ctrip.ops.sysdev.render.TemplateRender;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.log4j.Logger;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;


public class Elasticsearch extends BaseOutput {
    private static final Logger logger = Logger.getLogger(Elasticsearch.class
            .getName());

    private final static int BULKACTION = 20000;
    private final static int BULKSIZE = 15; //MB
    private final static int FLUSHINTERVAL = 10;
    private final static int CONCURRENTREQSIZE = 0;
    private final static boolean DEFAULTSNIFF = true;
    private final static boolean DEFAULTCOMPRESS = false;

    private String index;
    private String indexTimezone;
    private BulkProcessor bulkProcessor;
    private TransportClient esclient;
    private TemplateRender indexTypeRender;
    private TemplateRender idRender;
    private TemplateRender parentRender;

    public Elasticsearch(Map config) {
        super(config);
    }
    public Elasticsearch(Map config,List<BaseOutput> errorOutputProcessors) {
        super(config,errorOutputProcessors);
    }


    @Override
    protected void prepare() {
        this.index = (String) config.get("index");

        if (config.containsKey("timezone")) {
            this.indexTimezone = (String) config.get("timezone");
        } else {
            this.indexTimezone = "UTC";
        }

        if (config.containsKey("document_id")) {
            String document_id = config.get("document_id").toString();
            try {
                this.idRender = TemplateRender.getRender(document_id);
            } catch (IOException e) {
                logger.fatal("could not build tempalte from " + document_id);
                System.exit(1);
            }
        } else {
            this.idRender = null;
        }

        String index_type = "logs";
        if (config.containsKey("index_type")) {
            index_type = config.get("index_type").toString();
        }
        try {
            this.indexTypeRender = TemplateRender.getRender(index_type);
        } catch (IOException e) {
            logger.fatal("could not build tempalte from " + index_type);
            System.exit(1);
        }

        if (config.containsKey("document_parent")) {
            String document_parent = config.get("document_parent").toString();
            try {
                this.parentRender = TemplateRender.getRender(document_parent);
            } catch (IOException e) {
                logger.fatal("could not build tempalte from " + document_parent);
                System.exit(1);
            }
        } else {
            this.parentRender = null;
        }

        this.initESClient();

    }

    private void initESClient() throws NumberFormatException {

        String clusterName = (String) config.get("cluster");

        boolean sniff = config.containsKey("sniff") ? (boolean) config.get("sniff") : DEFAULTSNIFF;
        boolean compress = config.containsKey("compress") ? (boolean) config.get("compress") : DEFAULTCOMPRESS;

        Settings.Builder settings = Settings.builder()
                .put("client.transport.sniff", sniff)
                .put("transport.tcp.compress", compress)
                .put("cluster.name", clusterName);


        if (config.containsKey("settings")) {
            HashMap<String, Object> otherSettings = (HashMap<String, Object>) this.config.get("settings");
            otherSettings.entrySet().stream().forEach(entry -> settings.put(entry.getKey(), entry.getValue()));
        }
        esclient = new PreBuiltTransportClient(settings.build());

        ArrayList<String> hosts = (ArrayList<String>) config.get("hosts");
        hosts.stream().map(host -> host.split(":")).forEach(parsedHost -> {
            try {
                String host = parsedHost[0];
                String port = parsedHost.length == 2 ? parsedHost[1] : "9300";
                esclient.addTransportAddress(new InetSocketTransportAddress(
                        InetAddress.getByName(host), Integer.parseInt(port)));
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
        });

        int bulkActions = config.containsKey("bulk_actions") ? (int) config.get("bulk_actions") : BULKACTION;
        int bulkSize = config.containsKey("bulk_size") ? (int) config.get("bulk_size") : BULKSIZE;
        int flushInterval = config.containsKey("flush_interval") ? (int) config.get("flush_interval") : FLUSHINTERVAL;
        int concurrentRequests = config.containsKey("concurrent_requests") ? (int) config.get("concurrent_requests") : CONCURRENTREQSIZE;

        bulkProcessor = BulkProcessor.builder(
                esclient,
                new BulkProcessor.Listener() {
                    @Override
                    public void beforeBulk(long executionId, BulkRequest request) {
                        logger.info("executionId: " + executionId);
                        logger.info("numberOfActions: " + request.numberOfActions());
                        logger.debug("Hosts:" + esclient.transportAddresses().toString());
                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest request,
                                          BulkResponse response) {
                        logger.info("bulk done with executionId: " + executionId);
                        List<DocWriteRequest> requests = request.requests();

                        List<Map<String,Object>> errorEvents = new ArrayList<>();
                        //有请求失败
                        if (response.hasFailures()) {
                            for (BulkItemResponse item : response.getItems()) {
                                if (item.isFailed()) {
                                    DocWriteRequest writeRequest = requests.get(item.getItemId());
                                    if (writeRequest instanceof IndexRequest) {
                                        errorEvents.add(((IndexRequest) writeRequest).sourceAsMap());
                                    }
                                }
                            }
                            if (CollectionUtils.isNotEmpty(errorEvents)) {
                                processError(errorEvents);
                            }
                        }



                        /*int toBeTry = 0;
                        int totalFailed = 0;
                        for (BulkItemResponse item : response.getItems()) {
                            if (item.isFailed()) {
                                switch (item.getFailure().getStatus()) {
                                    case TOO_MANY_REQUESTS:
                                    case SERVICE_UNAVAILABLE:
                                        if (toBeTry == 0) {
                                            logger.error("bulk has failed item which NEED to retry");
                                            logger.error(item.getFailureMessage());
                                        }
                                        toBeTry++;
                                        bulkProcessor.add(requests.get(item.getItemId()));
                                        break;
                                    default:
                                        if (totalFailed == 0) {
                                            logger.error("bulk has failed item which do NOT need to retry");
                                            logger.error(item.getFailureMessage());
                                        }
                                        break;
                                }

                                totalFailed++;
                            }
                        }

                        if (totalFailed > 0) {
                            logger.info(totalFailed + " doc failed, " + toBeTry + " need to retry");
                        } else {
                            logger.debug("no failed docs");
                        }

                        if (toBeTry > 0) {
                            try {
                                logger.info("sleep " + toBeTry / 2
                                        + "millseconds after bulk failure");
                                Thread.sleep(toBeTry / 2);
                            } catch (InterruptedException e) {
                                logger.error(e);
                            }
                        } else {
                            logger.debug("no docs need to retry");
                        }*/

                    }
                    @Override
                    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                        logger.error("bulk got exception: " + failure.getMessage());
                        if (CollectionUtils.isNotEmpty(errorOutputProcessors)) {
                            List<Map<String,Object>> errorEvents = new ArrayList<>();
                            request.requests().forEach(docWriteRequest -> {
                                IndexRequest indexRequest = (IndexRequest)docWriteRequest;
                                errorEvents.add(indexRequest.sourceAsMap());
                            });
                            processError(errorEvents);
                        }
                    }
                }).setBulkActions(bulkActions)
                .setBulkSize(new ByteSizeValue(bulkSize, ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueSeconds(flushInterval))
                .setBackoffPolicy(
                        BackoffPolicy
                                .exponentialBackoff(TimeValue.timeValueMillis(100),3)
                )
                .setConcurrentRequests(concurrentRequests)
                .build();
    }

    protected void emit(final Map event) {
        String _index = DateFormatter.format(event, index, indexTimezone);
        String _indexType = indexTypeRender.render(event).toString();
        IndexRequest indexRequest;
        if (this.idRender == null) {
            indexRequest = new IndexRequest(_index, _indexType).source(event);
        } else {
            String _id = (String) idRender.render(event);
            indexRequest = new IndexRequest(_index, _indexType, _id).source(event);
        }
        if (this.parentRender != null) {
            indexRequest.parent(parentRender.render(event).toString());
        }
        BulkProcessor processor = this.bulkProcessor.add(indexRequest);
    }

    public void shutdown() {
        logger.info("flush docs and then shutdown");

        //flush immediately
        this.bulkProcessor.flush();

        // await for some time for rest data from input
        int flushInterval = 10;
        if (config.containsKey("flush_interval")) {
            flushInterval = (int) config.get("flush_interval");
        }
        try {
            this.bulkProcessor.awaitClose(flushInterval, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("failed to bulk docs before shutdown");
            logger.error(e);
        }
    }
}
