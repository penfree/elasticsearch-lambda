package bdmd.hadoop;
import net.sf.json.JSONObject;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;

import com.inin.analytics.elasticsearch.BaseESReducer;
import com.inin.analytics.elasticsearch.BaseESReducer.JOB_COUNTER;

public class OfflineESReducer extends BaseESReducer {

	@Override
	public String getTemplate() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getTemplateName() {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public void reduce(Text docMetaData, Iterator<Text> documentPayloads, OutputCollector<NullWritable, Text> output, final Reporter reporter) throws IOException {
		String[] pieces = StringUtils.split(docMetaData.toString(), TUPLE_SEPARATOR);
		String indexName = pieces[0];
		String routing = pieces[1]; 
		init(indexName);

		long start = System.currentTimeMillis();
		int count = 0;
		while(documentPayloads.hasNext()) {
			count++;
			Text line = documentPayloads.next();
			if(line == null) {
				continue;
			}
			json = line.toString();
			JSONObject obj = JSONObject.fromObject(json);
			indexType = obj.getString("_yida_es_type");
			docId = obj.getString("_yida_es_id");
			IndexRequestBuilder request = esEmbededContainer.getNode().client().prepareIndex(indexName, indexType).setId(docId).setRouting(routing);
			if (obj.has("_yida_es_parent_id"))
			{
				String parentId = obj.getString("_yida_es_parent_id");
				request.setParent(parentId);
			}

			IndexResponse response = request.setSource(json).execute().actionGet();
			if(response.isCreated()) {
				reporter.incrCounter(JOB_COUNTER.INDEX_DOC_CREATED, 1l);
			} else {
				reporter.incrCounter(JOB_COUNTER.INDEX_DOC_NOT_CREATED, 1l);
			}
		}

		reporter.incrCounter(JOB_COUNTER.TIME_SPENT_INDEXING_MS, System.currentTimeMillis() - start);
		
		snapshot(indexName, reporter);
		output.collect(NullWritable.get(), new Text(indexName));
	}

}
