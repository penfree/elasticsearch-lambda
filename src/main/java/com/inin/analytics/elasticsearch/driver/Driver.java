package com.inin.analytics.elasticsearch.driver;


import org.apache.hadoop.util.ToolRunner;

import bdmd.hadoop.OfflineIndexBuilder;

public class Driver{


    public static void main(String[] args) throws Throwable {   	
		// TODO Auto-generated method stub
		if (args.length < 1)
		{
			System.out.println("no parameter");
			return;
		}
		String [] newargs = new String [args.length-1];
		for (int i = 1; i < args.length; i++)
		{
			newargs[i-1] = args[i];
		}
		if (args[0].equals("OfflineIndexBuilder"))
		{
			int ret = ToolRunner.run(new OfflineIndexBuilder(), newargs);
			System.exit(ret);
		}
    }
}
