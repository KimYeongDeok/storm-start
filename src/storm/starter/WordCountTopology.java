package storm.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.ShellBolt;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import redis.clients.jedis.Jedis;
import storm.starter.util.MongoConnection;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class WordCountTopology {
    public static class WordSpout extends BaseRichSpout {
        SpoutOutputCollector _collector;
        Random _rand;


        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            _collector = collector;
            _rand = new Random();
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("word"));
        }

        @Override
        public void nextTuple() {
//            String text = "word1" +
//                        " word2 word2" +
//                        " word3 word3 word3" +
//                        " word4 word4 word4 word4";
            String text = "1 2";

            _collector.emit(new Values(text));
            Utils.sleep(1000);
        }
    }

    public static class SplitSentence extends ShellBolt implements IRichBolt {

        public SplitSentence() {
            super("python", "splitsentence.py");
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }

    public static class WordCountForRedis extends BaseBasicBolt {

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            Jedis jedis = new Jedis("61.43.139.65");

            String word = tuple.getString(0);

            word = word + this.hashCode();
            Long count = jedis.incr(word);
            collector.emit(new Values(word, count));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }

    }

    public static class WordCountForJava implements IRichBolt {
        Integer id;
        String name;
        Map<String, Integer> counters;
        private OutputCollector collector;


        @Override
        public void prepare(Map stormConf, TopologyContext context,
                            OutputCollector collector) {
            this.counters = new HashMap<String, Integer>();
            this.collector = collector;
            this.name = context.getThisComponentId();
            this.id = context.getThisTaskId();
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }

        @Override
        public void execute(Tuple input) {
            String str = input.getString(0);
            if (!counters.containsKey(str)) {
                counters.put(str, 1);
            } else {
                Integer c = counters.get(str) + 1;
                counters.put(str, c);
            }
            //Set the tuple as Acknowledge
            collector.ack(input);
        }

        @Override
        public void cleanup() {
            for (Map.Entry<String, Integer> entry : counters.entrySet()) {
                MongoConnection.insert
                        (entry.getKey(), String.valueOf(entry.getValue()));
            }
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new WordSpout(), 5);

        builder.setBolt("split", new SplitSentence(), 8)
                .shuffleGrouping("spout");

        builder.setBolt("word-count", new WordCountForJava(), 12)
//                .shuffleGrouping("split");
//                .fieldsGrouping("split", new Fields("word"));
//                  .allGrouping("split");
//                .directGrouping("split");
//        .noneGrouping("split");
        .globalGrouping("spout");

        Config conf = new Config();
        conf.setDebug(true);


        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", conf, builder.createTopology());

            Thread.sleep(10000);

            cluster.shutdown();
        }
    }
}
