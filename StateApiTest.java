package test.trident;

import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.Filter;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.operation.builtin.Count;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;

public class StateApiTest {
	private static final String Field_Sentence 	= "Field_Sentence";
	private static final String Field_Word 		= "Field_Word";
	private static final String Field_Count 	= "Field_Count";
	
	public static void main(String[] args) {
		System.out.println("\n〓〓〓〓〓〓〓〓〓   开始 〓〓〓〓〓〓〓〓\n");	
		
		@SuppressWarnings("unchecked")
		FixedBatchSpout spout = new FixedBatchSpout(new Fields(Field_Sentence), 3,
				new Values("Hello World"), new Values("I'm really a newbie to Storm and trident"),
				new Values("Hello World"), new Values("I'm really a newbie to Storm and trident"),
				new Values("Hello World"), new Values("I'm really a newbie to Storm and trident"),
				new Values("Hello World"), new Values("I'm really a newbie to Storm and trident"));
		spout.setCycle(false);
		
		TridentTopology topology = new TridentTopology();
		topology.newStream("TheSpout", spout)
				.each(new Fields(Field_Sentence), new Split(), new Fields(Field_Word))
				.groupBy(new Fields(Field_Word))
				.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields(Field_Count))
				.parallelismHint(6)
				.newValuesStream()
				.each(new Fields(Field_Word, Field_Count), new PrinterFilter());
		
		Config config = new Config();
		config.setDebug(false);
		config.setNumWorkers(1);
		
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("〓〓〓〓〓〓〓〓〓〓我的trident topology", config, topology.build());
		
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		cluster.shutdown();
		
		System.out.println("\n〓〓〓〓〓〓〓〓〓   结束 〓〓〓〓〓〓〓〓\n");
		
	}
	
	public static class Split extends BaseFunction {
		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			for (String word : tuple.getStringByField(Field_Sentence).split(" "))
				collector.emit(new Values(word));
		}
	}
	
	public static class PrinterFilter implements Filter {
		@Override
		public void prepare(Map conf, TridentOperationContext context) {
						
		}

		@Override
		public void cleanup() {
					
		}

		@Override
		public boolean isKeep(TridentTuple tuple) {
			System.out.println("\n■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■");
			System.out.println(tuple.getStringByField(Field_Word) + ":" + tuple.getLongByField(Field_Count) + "\n");
			return true;
		}
		
	}
}
