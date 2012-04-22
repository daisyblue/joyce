package joyce;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import joyce.bolt.FilterVideos;
import joyce.bolt.RedisBolt;
import joyce.spout.TwitterSampleSpout;

public class RollingTopVideos {

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("tweet", new TwitterSampleSpout());

        builder.setBolt("vtweet", new FilterVideos()).shuffleGrouping("tweet");
        builder.setBolt("redis", new RedisBolt()).shuffleGrouping("vtweet");

        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("joyce-demo", conf, builder.createTopology());
        Thread.sleep(10000);

        cluster.killTopology("joyce-demo");
        cluster.shutdown();

    }
}
