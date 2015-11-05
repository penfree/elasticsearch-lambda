package bdmd.hadoop;
import net.sf.json.JSONObject;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.inin.analytics.elasticsearch.BaseESReducer;
import com.inin.analytics.elasticsearch.ESEmbededContainer;
import com.inin.analytics.elasticsearch.BaseESReducer.JOB_COUNTER;

public class OfflineESReducer extends BaseESReducer {
	protected String template = "";
	private static transient Logger logger = LoggerFactory.getLogger(OfflineESReducer.class);
	@Override
	public String getTemplate() {
		// TODO Auto-generated method stub
		return template;
	}

	@Override
	public String getTemplateName() {
		// TODO Auto-generated method stub
		return "template";
	}
	public static void copyFile(File sourcefile,File targetFile) throws IOException{

		//新建文件输入流并对它进行缓冲
		FileInputStream input=new FileInputStream(sourcefile);
		BufferedInputStream inbuff=new BufferedInputStream(input);

		//新建文件输出流并对它进行缓冲
		FileOutputStream out=new FileOutputStream(targetFile);
		BufferedOutputStream outbuff=new BufferedOutputStream(out);

		//缓冲数组
		byte[] b=new byte[1024*5];
		int len=0;
		while((len=inbuff.read(b))!=-1){
			outbuff.write(b, 0, len);
		}

		//刷新此缓冲的输出流
		outbuff.flush();

		//关闭流
		inbuff.close();
		outbuff.close();
		out.close();
		input.close();
		logger.warn("copy " + sourcefile + " to " + targetFile);
	}

	public static void copyDirectory(String sourceDir,String targetDir) throws IOException{

		//新建目标目录

		(new File(targetDir)).mkdirs();

		//获取源文件夹当下的文件或目录
		File[] file=(new File(sourceDir)).listFiles();

		for (int i = 0; i < file.length; i++) {
			if(file[i].isFile()){
				//源文件
				File sourceFile=file[i];
				//目标文件
				File targetFile=new File(new File(targetDir).getAbsolutePath()+File.separator+file[i].getName());      
				copyFile(sourceFile, targetFile);        
			}
			if(file[i].isDirectory()){
				//准备复制的源文件夹
				String dir1=sourceDir+ File.separator + file[i].getName();
				//准备复制的目标文件夹
				String dir2=targetDir+ File.separator +file[i].getName();
				copyDirectory(dir1, dir2);
			}
		}

	}
	
	public int copyExternalFiles()
	{
		String configDir = esHomeDir + File.separator + "config";
		String pluginDir = esHomeDir + File.separator + "plugin";
		String ikConfig = "ik-config.tgz";
		String plugin = "elasticsearch-analysis-ik.tgz";
		try {
			if (new File(ikConfig).exists())
			{
				copyDirectory(ikConfig, configDir);
			}
			if (new File(plugin).exists())
			{
				copyDirectory(plugin, pluginDir);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}	
	
	@Override
	public void configure(JobConf job) {
		// TODO Auto-generated method stub
		super.configure(job);
		template = job.get("es.template");
		this.copyExternalFiles();
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
