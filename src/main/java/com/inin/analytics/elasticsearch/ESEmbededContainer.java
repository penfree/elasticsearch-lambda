package com.inin.analytics.elasticsearch;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapred.Reporter;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
//import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.PluginsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Builds an embedded elasticsearch instance and configures it for you
 * 
 * @author drew
 *
 */
public class ESEmbededContainer {
	private Node node;
	private long DEFAULT_TIMEOUT_MS = 60 * 5 * 1000; 
	private static Integer MAX_MERGED_SEGMENT_SIZE_MB = 256;
	private static transient Logger logger = LoggerFactory.getLogger(ESEmbededContainer.class);
	
	public void snapshot(List<String> index, String snapshotName, String snapshotRepoName, Reporter reporter) {
		snapshot(index, snapshotName, snapshotRepoName, DEFAULT_TIMEOUT_MS, reporter);
	}
	
	/**
	 * Flush, optimize, and snapshot an index. Block until complete. 
	 * 
	 * @param index
	 * @param snapshotName
	 * @param snapshotRepoName
	 */
	public void snapshot(List<String> indicies, String snapshotName, String snapshotRepoName, long timeoutMS, Reporter reporter) {
		/* Flush & optimize before the snapshot.
		 *  
		 * TODO: Long operations could block longer that the container allows an operation to go
		 * unresponsive b/f killing. We need to issue the request and poll the future waiting on the
		 * operation to succeed, but update a counter or something to let the hadoop framework
		 * know the process is still alive. 
		 */  
		TimeValue v = new TimeValue(timeoutMS);
		for(String index : indicies) {
			long start = System.currentTimeMillis();

			// Flush
			node.client().admin().indices().prepareFlush(index).get(v);
			if(reporter != null) {
				reporter.incrCounter(BaseESReducer.JOB_COUNTER.TIME_SPENT_FLUSHING_MS, System.currentTimeMillis() - start);
			}

			// Merge
			start = System.currentTimeMillis();
			node.client().admin().indices().prepareOptimize(index).get(v);
			if(reporter != null) {
				reporter.incrCounter(BaseESReducer.JOB_COUNTER.TIME_SPENT_MERGING_MS, System.currentTimeMillis() - start);
			}
		}

		// Snapshot
		long start = System.currentTimeMillis();
		node.client().admin().cluster().prepareCreateSnapshot(snapshotRepoName, snapshotName).setWaitForCompletion(true).setIndices((String[]) indicies.toArray(new String[0])).get();
		if(reporter != null) {
			reporter.incrCounter(BaseESReducer.JOB_COUNTER.TIME_SPENT_SNAPSHOTTING_MS, System.currentTimeMillis() - start);
		}

	}
	
	public void deleteSnapshot(String snapshotName, String snapshotRepoName) {
		node.client().admin().cluster().prepareDeleteSnapshot(snapshotRepoName, snapshotName).execute().actionGet();
	}

	public static class Builder {
		private ESEmbededContainer container;
		private String nodeName;
		private Integer numShardsPerIndex;
		private String workingDir;
		private String clusterName;
		private String templateName;
		private String templateSource;
		private String snapshotWorkingLocation;
		private String snapshotRepoName;
		private boolean memoryBackedIndex = false;
		private String homeDir;
		


		public ESEmbededContainer build() {
			Preconditions.checkNotNull(nodeName);
			Preconditions.checkNotNull(numShardsPerIndex);
			Preconditions.checkNotNull(workingDir);
			Preconditions.checkNotNull(clusterName);
			//this.copyExternalFiles();

			org.elasticsearch.common.settings.Settings.Builder builder = org.elasticsearch.common.settings.Settings.builder()
			.put("http.enabled", false) // Disable HTTP transport, we'll communicate inner-jvm
			.put("processors", 1) // We could experiment ramping this up to match # cores - num reducers per node
			.put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, numShardsPerIndex) 
			.put("node.name", nodeName)
			.put("path.data", workingDir)
			.put("path.home", homeDir)
			//.put("plugins." + PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, true) // Allow plugins if they're bundled in with the uuberjar
			.put("index.refresh_interval", -1) 
			.put("index.translog.flush_threshold_size", "128mb") // Aggressive flushing helps keep the memory footprint below the yarn container max. TODO: Make configurable 
			.put("bootstrap.mlockall", true)
			.put("cluster.routing.allocation.disk.watermark.low", 99) // Nodes don't form a cluster, so routing allocations don't matter
			.put("cluster.routing.allocation.disk.watermark.high", 99)
			.put("index.load_fixed_bitset_filters_eagerly", false)
			.put("indices.store.throttle.type", "none") // Allow indexing to max out disk IO
			.put("indices.memory.index_buffer_size", "5%") // The default 10% is a bit large b/c it's calculated against JVM heap size & not Yarn container allocation. Choosing a good value here could be made smarter.
			.put("index.merge.policy.max_merged_segment", MAX_MERGED_SEGMENT_SIZE_MB + "mb") // The default 5gb segment max size is too large for the typical hadoop node
			//.put("index.merge.policy.max_merge_at_once", 10) 
			.put("index.merge.policy.segments_per_tier", 4)
			.put("index.merge.scheduler.max_thread_count", 1)
			.put("path.repo", snapshotWorkingLocation)
			.put("index.analysis.analyzer.ik_max.type", "ik")
			.put("index.analysis.analyzer.ik_smart.type", "ik")
			.put("index.analysis.analyzer.ik_smart.use_smart", true)
			.put("index.compound_format", false) // Explicitly disable compound files
			//.put("index.codec", "best_compression") // Lucene 5/ES 2.0 feature to play with when that's out
			.put("indices.fielddata.cache.size", "0%");
			
			if(memoryBackedIndex) {
				builder.put("index.store.type", "memory");
			}
			Settings nodeSettings = builder.build();

			// Create the node
			container.setNode(nodeBuilder()
					.client(false) // It's a client + data node
					.local(true) // Tell ES cluster discovery to be inner-jvm only, disable HTTP based node discovery
					.clusterName(clusterName)
					.settings(nodeSettings)
					.build());

			// Start ES
			container.getNode().start();

			// Configure the cluster with an index template mapping
			if(templateName != null && templateSource != null) {
				container.getNode().client().admin().indices().preparePutTemplate(templateName).setSource(templateSource).get();	
			}

			// Create the snapshot repo
			if(snapshotWorkingLocation != null && snapshotRepoName != null) {
				Map<String, Object> settings = new HashMap<>();
				settings.put("location", snapshotWorkingLocation);
				settings.put("compress", true);
				settings.put("max_snapshot_bytes_per_sec", "400mb"); // The default 20mb/sec is very slow for a local disk to disk snapshot
				container.getNode().client().admin().cluster().preparePutRepository(snapshotRepoName).setType("fs").setSettings(settings).get();
			}

			return container;
		}

		public Builder() {
			container = new ESEmbededContainer();
		}

		public Builder withNodeName(String nodeName) {
			this.nodeName = nodeName;
			return this;
		}

		public Builder withNumShardsPerIndex(Integer numShardsPerIndex) {
			this.numShardsPerIndex = numShardsPerIndex;
			return this;
		}

		public Builder withWorkingDir(String workingDir) {
			this.workingDir = workingDir;
			return this;
		}
		
		public Builder withHomeDir(String homeDir) {
			this.homeDir = homeDir;
			return this;
		}
		public Builder withClusterName(String clusterName) {
			this.clusterName = clusterName;
			return this;
		}

		public Builder withTemplate(String templateName, String templateSource) {
			this.templateName = templateName;
			this.templateSource = templateSource;
			return this;
		}

		public Builder withSnapshotWorkingLocation(String snapshotWorkingLocation) {
			this.snapshotWorkingLocation = snapshotWorkingLocation;
			return this;
		}

		public Builder withSnapshotRepoName(String snapshotRepoName) {
			this.snapshotRepoName = snapshotRepoName;
			return this;
		}
		
		public Builder withInMemoryBackedIndexes(boolean memoryBackedIndex) {
			this.memoryBackedIndex = memoryBackedIndex;
			return this;
		}

	}

	public Node getNode() {
		return node;
	}

	public void setNode(Node node) {
		this.node = node;
	}

}
