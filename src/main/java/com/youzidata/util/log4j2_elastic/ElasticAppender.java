package com.youzidata.util.log4j2_elastic;

import com.google.gson.Gson;
import java.io.Serializable;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.message.MapMessage;
import org.apache.logging.log4j.message.Message;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

@Plugin(name="ElasticAppender", category="Core", elementType="appender", printObject=true)
public class ElasticAppender
        extends AbstractAppender
{
    private static final long serialVersionUID = 1201362115669111175L;

    protected ElasticAppender(Config config, Filter filter, Layout<? extends Serializable> layout)
    {
        super(config.name, filter, layout);
        this.config = config;
        this.que = new ArrayBlockingQueue(config.buffer);
        this.level = config.level;
        LOGGER.info("Generated ElasticAppender " + config);
    }

    private static class Config
    {
        String name = "elastic";
        List<URI> uri = null;
        String cluster = null;
        String index = "logstash";
        String type = "log";
        RotateIndexType rotateIndexParam = RotateIndexType.DAY;
        String node = "local";
        String service = "java";
        int buffer = 50000;
        long expiry = -1L;
        Level level;

        public String toString()
        {
            return "Config [name=" + this.name + ", uri=" + this.uri + ", cluster=" + this.cluster + ", index=" + this.index + ", type=" + this.type + ", rotateIndexParam=" + this.rotateIndexParam + ", node=" + this.node + ", service=" + this.service + ", buffer=" + this.buffer + ", level=" + this.level + "]";
        }
    }

    private static String getProp(String param, String name, String def)
    {
        if (param != null) {
            return param;
        }
        String str = null;
        str = System.getProperty(name);
        if (str != null) {
            return str;
        }
        str = System.getenv(name);
        if (str != null) {
            return str;
        }
        return def;
    }

    @PluginFactory
    public static synchronized ElasticAppender createAppender(@PluginAttribute("name") String name, @PluginAttribute("uri") String elastic_local, @PluginAttribute("cluster") String elastic_cluster, @PluginAttribute(value="index") String index, @PluginAttribute(value="indexRotate", defaultString="DAY") String indexRotate, @PluginAttribute(value="type", defaultString="logs") String type, @PluginAttribute("node") String node, @PluginAttribute("service") String service, @PluginAttribute(value="bufferSize", defaultInt=5000) Integer bufferSize, @PluginAttribute(value="expiryTime", defaultLong=-1L) Long expiry, @PluginAttribute(value="expiryUnit", defaultString="d") String expiryUnit, @PluginAttribute("level") Level level, @PluginAttribute("levelin") Level levelin, @PluginElement("Filters") Filter filter)
    {
        LOGGER.info("Create new Elastic Appender Version 3.1.0");

        Config config = new Config();
        if (name == null)
        {
            LOGGER.error("No name provided for StubAppender");
            return null;
        }
        config.name = name;
        String myhost = "Unknown";
        try
        {
            myhost = Inet4Address.getLocalHost().getHostAddress().toString();
        }
        catch (UnknownHostException e) {}
        config.node = getProp(node, "node", myhost);
        config.service = getProp(service, "service", "Java");
        config.cluster = getProp(elastic_cluster, "elastic_cluster", "elasticsearch");
        config.index = getProp(elastic_cluster, "elastic_index", "logstash");
        elastic_local = getProp(elastic_local, "elastic_local", "native://localhost:9300");

        String[] uriDat = elastic_local.split("\\,");
        config.uri = new LinkedList();
        for (String act : uriDat)
        {
            LOGGER.debug("Found following URI " + act);
            config.uri.add(URI.create(act));
        }
        config.rotateIndexParam = RotateIndexType.NO;
        if (indexRotate == null)
        {
            config.rotateIndexParam = RotateIndexType.DAY;
        }
        else if (indexRotate.equalsIgnoreCase("NO"))
        {
            config.rotateIndexParam = RotateIndexType.NO;
        }
        else if (indexRotate.equalsIgnoreCase("DAY"))
        {
            config.rotateIndexParam = RotateIndexType.DAY;
        }
        else if (indexRotate.equalsIgnoreCase("HOUR"))
        {
            config.rotateIndexParam = RotateIndexType.HOUR;
        }
        else
        {
            LOGGER.warn("Illegal type for indexRotate only support NO,DAY,HOUR - default to DAY");
            config.rotateIndexParam = RotateIndexType.DAY;
        }
        if (level == null) {
            config.level = Level.ALL;
        } else {
            config.level = level;
        }
        if (index != null) {
            config.index = index;
        }
        if (type != null) {
            config.type = type;
        }
        if (bufferSize != null) {
            config.buffer = bufferSize.intValue();
        }
        long factor = 86400000L;
        if (expiryUnit != null) {
            if (expiryUnit.equalsIgnoreCase("w")) {
                factor = 604800000L;
            } else if (expiryUnit.equalsIgnoreCase("d")) {
                factor = 86400000L;
            } else if (expiryUnit.equalsIgnoreCase("h")) {
                factor = 3600000L;
            } else if (expiryUnit.equalsIgnoreCase("m")) {
                factor = 60000L;
            } else if (expiryUnit.equalsIgnoreCase("s")) {
                factor = 1000L;
            } else if (expiryUnit.equalsIgnoreCase("msec")) {
                factor = 1000L;
            }
        }
        if ((expiry != null) &&
                (expiry.longValue() > 0L)) {
            config.expiry = (expiry.longValue() * factor);
        }
        return new ElasticAppender(config, filter, null);
    }

    volatile Level level = Level.ALL;
    volatile Map<String, Level> special = new HashMap();

    static class InternalLogEvent
    {
        String threadName = "";
        LogEvent eve;

        InternalLogEvent(LogEvent event)
        {
            this.eve = event;
            this.threadName = this.eve.getThreadName();
        }
    }

    ArrayBlockingQueue<InternalLogEvent> que = null;

    public void append(LogEvent event)
    {
        Level spec = (Level)this.special.get(event.getLoggerName());
        if (spec != null)
        {
            if (event.getLevel().isMoreSpecificThan(spec)) {}
        }
        else if (!event.getLevel().isMoreSpecificThan(this.level)) {
            return;
        }
        this.que.offer(new InternalLogEvent(event));
    }

    volatile Config config = null;
    volatile boolean running = true;
    private static final String LOGCONFIG = "log3config";
    private static final String LOGTYPE_MAIN = "main";
    private static final String LOGTYPE_FILTER = "filter";

    public void start()
    {
        this.running = true;
        Thread thr = new Thread()
        {
            public void run()
            {
                try {
                    ElasticAppender.this.worker();
                } catch (UnknownHostException e) {
                    e.printStackTrace();
                }
            }
        };
        thr.setDaemon(true);
        thr.setName("ElasticAppender " + this.config.name);
        thr.start();
        setStarted();
    }

    public void stop()
    {
        this.running = false;
    }

    void worker() throws UnknownHostException {
        Settings settings = Settings.builder()
                .put("cluster.name", this.config.cluster)
                .put("network.server", false)
//                .put("node.client", true)
                .put("client.transport.sniff", false)
                .put("client.transport.ping_timeout", "30s")
                .put("client.transport.ignore_cluster_name", false)
                .put("client.transport.nodes_sampler_interval", "30s")
                .build();

        Logger logger = LogManager.getLogger("ElasticAppender " + this.config.name);
        logger.info("Start now ElasticAppender Thread");

        String resolvedIndex = null;
        long indexHour = 0L;
        SimpleDateFormat formatHour = new SimpleDateFormat("yyyy.MM.dd.HH");
        SimpleDateFormat formatDay = new SimpleDateFormat("yyyy.MM.dd");

        SimpleDateFormat formatElastic = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

        Map<String, Object> eve = new HashMap();
        while (this.running)
        {
            TransportClient client = new PreBuiltTransportClient(settings);
            for (URI act : this.config.uri) {
                client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(act.getHost()), act.getPort()));
            }
            if (client.connectedNodes().isEmpty())
            {
                client.close();
                logger.error("unable to connect to Elasticsearch cluster");
                try
                {
                    Thread.sleep(1000L);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            else
            {
                while (this.running)
                {
                    InternalLogEvent ilog = null;
                    try
                    {
                        ilog = (InternalLogEvent)this.que.poll(20L, TimeUnit.SECONDS);
                    }
                    catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    if (ilog != null)
                    {
                        List<InternalLogEvent> logs = new LinkedList();
                        logs.add(ilog);
                        this.que.drainTo(logs, 500);
                        BulkRequestBuilder bulkRequest = client.prepareBulk();
                        for (InternalLogEvent ievent : logs)
                        {
                            LogEvent event = ievent.eve;
                            if (event.getTimeMillis() / 3600000L != indexHour)
                            {
                                indexHour = event.getTimeMillis() / 3600000L;
                                long localTime;
                                Date dd;
                                switch (this.config.rotateIndexParam)
                                {
                                    case HOUR:
                                        localTime = event.getTimeMillis();
                                        dd = new Date(localTime - TimeZone.getDefault().getOffset(localTime));
                                        resolvedIndex = this.config.index + "-" + formatHour.format(dd);
                                        break;
                                    case DAY:
                                        localTime = event.getTimeMillis();
                                        dd = new Date(localTime - TimeZone.getDefault().getOffset(localTime));
                                        resolvedIndex = this.config.index + "-" + formatDay.format(dd);
                                        break;
                                    case NO:
                                        resolvedIndex = this.config.index;
                                }
                            }
                            eve.clear();
                            if ((event.getMessage() instanceof MapMessage)) {
                                eve.putAll(((MapMessage)event.getMessage()).getData());
                            } else {
                                eve.put("message", event.getMessage().getFormattedMessage());
                            }
                            eve.put("@source", this.config.service);
                            eve.put("host", this.config.node);
                            eve.put("@version", Integer.valueOf(1));
                            eve.put("@timestamp", formatElastic.format(new Date(event.getTimeMillis())));
                            eve.put("level", event.getLevel().toString());
                            eve.put("logger", event.getLoggerName());
                            eve.put("loggerFQDN", event.getLoggerFqcn());
                            if (event.getMarker() != null) {
                                eve.put("marker", event.getMarker().toString());
                            }
                            eve.put("thread", ievent.threadName);
                            if (event.getSource() != null) {
                                eve.put("stack", event.getSource().toString());
                            }
                            if (event.getThrown() != null) {
                                eve.put("throw", convThrowable(event.getThrown()));
                            }
                            eve.put("context", event.getContextMap());
                            IndexRequestBuilder d = client.prepareIndex(resolvedIndex, this.config.type).setSource(eve);
                            if (this.config.expiry > 0L) {
                                d.setTTL(this.config.expiry);
                            }
                            bulkRequest.add(d);
                        }
                        bulkRequest.execute();
                    }
                }
            }
        }
        setStopped();
    }

    private String convThrowable(Throwable t)
    {
        StringBuilder result = new StringBuilder();
        result.append(t.toString());
        result.append('\n');
        for (StackTraceElement element : t.getStackTrace())
        {
            result.append(element);
            result.append('\n');
        }
        result.append('\n');
        if (t.getCause() != null)
        {
            result.append("Caused by ...\n");
            result.append(convThrowable(t.getCause()));
        }
        return result.toString();
    }

    private static abstract class DAO
    {
        void write(TransportClient client, Gson gson)
        {
            client.prepareIndex("log3config", getType()).setId(getId()).setSource(gson.toJson(this)).get();
//            ((IndexRequestBuilder)new IndexRequestBuilder(client).setIndex("log3config")).setType(getType()).setId(getId()).setSource(gson.toJson(this)).get();
        }

        protected abstract String getType();

        protected abstract String getId();
    }

    private static class DAO_Filter
            extends ElasticAppender.DAO
    {
        String node;
        String service;
        String name;
        String level;

        protected String getType()
        {
            return "filter";
        }

        private DAO_Filter()
        {
            super();
            this.node = "dummy";
            this.service = "dummy";
            this.name = "dummy";
            this.level = Level.ALL.name();
        }

        private DAO_Filter(String node, String service, String name, String level)
        {
            super();
            this.node = node;
            this.service = service;
            this.name = name;
            this.level = level;
        }

        protected String getId()
        {
            return this.node + "-" + this.service + "-" + this.name;
        }
    }

    private static class DAO_MainConfig
            extends ElasticAppender.DAO
    {
        String node;
        String service;
        String defaultLevel;
        String inLevel;

        protected String getType()
        {
            return "main";
        }

        DAO_MainConfig()
        {
            super();
            this.node = "dummy";
            this.service = "dummy";
            this.defaultLevel = Level.ALL.name();
        }

        private DAO_MainConfig(String node, String service, String defaultLevel)
        {
            super();
            this.node = node;
            this.service = service;
            this.defaultLevel = defaultLevel;
        }

        int refresh = 60;

        protected String getId()
        {
            return this.node + "-" + this.service;
        }
    }
}
