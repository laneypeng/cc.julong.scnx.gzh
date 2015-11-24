package com.apache.hbase.coprocessor;

import java.io.IOException;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;



public class IndexObserver extends BaseRegionObserver {

	private HTableInterface htable = null;
	
	@Override
	public void start(CoprocessorEnvironment e) throws IOException {
		super.start(e);
		if(htable == null){
			this.htable = e.getTable(TableName.valueOf("T1"));
		}
	}

	@Override
	public void prePut(ObserverContext<RegionCoprocessorEnvironment> e,
			Put put, WALEdit edit, Durability durability) throws IOException {
		Put newPut = new Put(put.getRow());
		newPut.add("cf".getBytes(), "c1".getBytes(), "111111".getBytes());
		htable.put(newPut);		
	}

}
