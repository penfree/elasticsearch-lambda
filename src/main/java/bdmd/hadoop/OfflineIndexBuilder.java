package bdmd.hadoop;
import static org.kohsuke.args4j.ExampleMode.ALL;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.StringArrayOptionHandler;

import com.inin.analytics.elasticsearch.BaseESMapper;
import com.inin.analytics.elasticsearch.ConfigParams;
import com.inin.analytics.elasticsearch.IndexingPostProcessor;
import com.inin.analytics.elasticsearch.example.ExampleIndexingJob;
import com.inin.analytics.elasticsearch.example.ExampleIndexingReducerImpl;
public class OfflineIndexBuilder implements Tool {
	private Configuration conf;
	@Option(name = "-i", handler = StringArrayOptionHandler.class, required = true, usage = "input files, allow multiple input")
	List<String> inputPath ;
	@Option(name = "--snap-work-dir", usage = "snapshotWorkingLocation", required = true)
	String snapshotWorkingLocation;
	@Option(name = "--snap-dest", usage = "snapshotFinalDestination", required = true)
	String snapshotFinalDestination;
	@Option(name = "--snap-repo", usage = "snapshotRepoName", required = true)
	String snapshotRepoName;
	@Option(name = "--es-work-dir", usage = "esWorkingDir", required = true)
	String esWorkingDir ;
	@Option(name = "--num-reduce", usage = "numReducers")
	int numReducers = 1;
	@Option(name = "--num-shards", usage = "numShardsPerIndex")
	int numShardsPerIndex = 1;
	@Option(name = "--manifest", usage = "manifestLocation", required = true)
	String manifestLocation;
	@Option(name = "--index", usage = "index name", required = true)
	String esIndexName;
	@Option(name = "--template", usage = "template file path", required = true)
	String template;
	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

    /**
     * 以行为单位读取文件，常用于读面向行的格式化文件
     */
    public String readFile(String fileName) {
        File file = new File(fileName);
        BufferedReader reader = null;
        String result = "";
        try {
            System.out.println("以行为单位读取文件内容，一次读一整行：");
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            // 一次读入一行，直到读入null为文件结束
            while ((tempString = reader.readLine()) != null) {
                // 显示行号
            	result = result + tempString;
                //System.out.println("line " + line + ": " + tempString);
                line++;
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
        return result;
    }
	
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
        CmdLineParser parser = new CmdLineParser(this);
         
        // if you have a wider console, you could increase the value;
        // here 80 is also the default
        parser.setUsageWidth(180);
 
        try {
            // parse the arguments.
            parser.parseArgument(args);
 
        } catch( CmdLineException e ) {
            // if there's a problem in the command line,
            // you'll get this exception. this will report
            // an error message.
            System.err.println(e.getMessage());
            System.err.println("OfflineIndexBuilder [options...] arguments...");
            // print the list of available options
            parser.printUsage(System.err);
            System.err.println();
 
            // print option sample. This is useful some time
            System.err.println(" Example: OfflineIndexBuilder "+parser.printExample(ALL));
 
            return -1;
        }


		// Remove trailing slashes from the destination 
		snapshotFinalDestination = StringUtils.stripEnd(snapshotFinalDestination, "/");

		conf = new Configuration();
		conf.set(ConfigParams.SNAPSHOT_WORKING_LOCATION_CONFIG_KEY.toString(), snapshotWorkingLocation);
		conf.set(ConfigParams.SNAPSHOT_FINAL_DESTINATION.toString(), snapshotFinalDestination);
		conf.set(ConfigParams.SNAPSHOT_REPO_NAME_CONFIG_KEY.toString(), snapshotRepoName);
		conf.set(ConfigParams.ES_WORKING_DIR.toString(), esWorkingDir);
		conf.set(ConfigParams.NUM_SHARDS_PER_INDEX.toString(), Integer.toString(numShardsPerIndex));
		conf.set("es.index.name", esIndexName);
		conf.set("es.template", readFile(template));
		System.err.println(conf.get("es.template"));
		//DistributedCache.addCacheFile(new Path(template), conf);
		JobConf job = new JobConf(conf, ExampleIndexingJob.class);
		
		job.setJobName("Elastic Search Offline Index Generator");
		job.setInputFormat(KeyValueTextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);
		job.setMapperClass(OfflineESMapper.class);
		job.setReducerClass(OfflineESReducer.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setNumReduceTasks(numReducers);
		job.setSpeculativeExecution(false);
		FileSystem fs = FileSystem.get(conf);
		Path jobOutput = new Path(manifestLocation + "/raw/");
		Path manifestFile = new Path(manifestLocation + "manifest");
		if (fs.exists(jobOutput))
		{
			fs.delete(jobOutput, true);
		}
		FileOutputFormat.setOutputPath(job, jobOutput);

		// Set up inputs
		for(String input : inputPath) {
			FileInputFormat.addInputPath(job, new Path(input));
		}

		JobClient.runJob(job);
		IndexingPostProcessor postProcessor = new IndexingPostProcessor();
		postProcessor.execute(jobOutput, manifestFile, esWorkingDir, numShardsPerIndex, conf, OfflineESReducer.class);
		return 0;
	}
}
