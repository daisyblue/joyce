package joyce.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import java.util.Map;
import redis.clients.jedis.Jedis;
import twitter4j.Status;

public class RedisBolt extends BaseRichBolt {

    private transient Jedis _redis;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        if (_redis == null) {
            _redis = new Jedis("localhost");
        }
    }

    @Override
    public void execute(Tuple tuple) {
        Status status = (Status)tuple.getValue(0);
        _redis.set("tweet", status.getText());
    }
}
