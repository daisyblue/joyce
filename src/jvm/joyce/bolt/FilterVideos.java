package joyce.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.Map;
import java.util.regex.Pattern;
import twitter4j.Status;
import twitter4j.URLEntity;

public class FilterVideos extends BaseRichBolt {
    
    private OutputCollector _collector;
    private Pattern _youtube = Pattern.compile("youtu.?be");
    
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        Status status = (Status)tuple.getValue(0);
        URLEntity urls[] = status.getURLEntities();
        
        if(urls != null && urls.length > 0) {
            for(URLEntity url: urls) {
                if(_youtube.matcher(url.getDisplayURL()).find()) {
                    _collector.emit(new Values(status));
                }
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
       declarer.declare(new Fields("vtweet"));
    }
    
}
