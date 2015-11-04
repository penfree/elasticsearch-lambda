package bdmd.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.inin.analytics.elasticsearch.BaseESMapper;
import com.inin.analytics.elasticsearch.BaseESReducer;
import com.inin.analytics.elasticsearch.index.rotation.ElasticSearchIndexMetadata;
import com.inin.analytics.elasticsearch.index.routing.ElasticsearchRoutingStrategy;
import com.inin.analytics.elasticsearch.index.routing.ElasticsearchRoutingStrategyV1;

import net.sf.json.JSONObject;

public class OfflineESMapper extends BaseESMapper {
	protected String indexName = "";
	private ElasticsearchRoutingStrategy elasticsearchRoutingStrategy;

	@Override
	public void configure(JobConf job) {
		indexName = job.get("es.index.name");
		int numShards = job.getInt("es.shards.num", 1);
		ElasticSearchIndexMetadata indexMetadata = new ElasticSearchIndexMetadata();
		indexMetadata.setNumShards(numShards);
		indexMetadata.setNumShardsPerOrg(1);
		elasticsearchRoutingStrategy = new ElasticsearchRoutingStrategyV1();
		elasticsearchRoutingStrategy.configure(indexMetadata);
	}
	public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		/**
		 * Reducer key looks like this   [indexName]|[routing hash] value [doc type]|[doc id]|json
		 * 
		 */
		JSONObject obj = JSONObject.fromObject(key.toString());
		String routingKey = obj.getString("_yida_es_routing_key");
		String routingHash = elasticsearchRoutingStrategy.getRoutingHash(routingKey, "es");
		Text outputKey = new Text(indexName + BaseESReducer.TUPLE_SEPARATOR + routingHash);
		output.collect(outputKey, value);
	} 
}
