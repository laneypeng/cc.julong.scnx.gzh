package com.apache.hbase.query;

import com.apache.hbase.query.util.PropertiesHelper;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

public class HBaseQuery implements Query
{
	private static final Logger LOG = Logger.getLogger(HBaseQuery.class);
	private String quorum;
	private int port;
	private String znodeParent;
	private String tablePrefix;
	private static Configuration conf = null;

	public HBaseQuery() {
		this.quorum = PropertiesHelper.getInstance().getValue("hbase.zookeeper.quorum");
		this.port = Integer.parseInt(PropertiesHelper.getInstance().getValue("hbase.zookeeper.property.clientPort"));
		this.znodeParent = PropertiesHelper.getInstance().getValue("zookeeper.znode.parent");
		this.tablePrefix = PropertiesHelper.getInstance().getValue("hbase.table.prefix");

		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", this.quorum);
		conf.set("hbase.zookeeper.property.clientPort", this.port+"");
		conf.set("zookeeper.znode.parent", "/" + this.znodeParent);
	}

	private List<String> generalScope(String start, String end)
			throws ParseException
	{
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		Date startDate = format.parse(start);
		Date endDate = format.parse(end);
		Calendar calendarTemp = Calendar.getInstance();
		calendarTemp.setTime(startDate);
		long newstart = calendarTemp.getTime().getTime();
		long et = endDate.getTime();

		List scopes = new ArrayList();
		while (newstart <= et) {
			String date = format.format(calendarTemp.getTime());
			String startTime = "";
			if (newstart == startDate.getTime())
				startTime = start;
			else {
				startTime = date + " 00:00:00";
			}
			String endTime = "";
			if (et == newstart)
				endTime = end;
			else {
				endTime = date + " 23:59:59";
			}
			calendarTemp.add(6, 1);
			newstart = calendarTemp.getTime().getTime();
			scopes.add(startTime + "#" + endTime);
		}
		return scopes;
	}

	private void exactMatch(QueryObject query, QueryStatusManager manager)
			throws Exception
	{
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat format1 = new SimpleDateFormat("yyyyMM");

		List<String> scopes = generalScope(query.getStart(), query.getEnd());
		HBaseAdmin hbaseadmin = null;
		try {
			for (String date : scopes) {
				String[] datas = date.split("#");
				String start = datas[0];
				Date day = format.parse(start);

				String tableName = this.tablePrefix + format1.format(day);
				hbaseadmin = new HBaseAdmin(conf);

				if (!hbaseadmin.tableExists(tableName)) {
					LOG.info(tableName + "表未找到");

					manager.setStatus(tableName, Boolean.valueOf(true));
				} else {
					manager.setStatus(tableName, Boolean.valueOf(false));

					Thread thread = new Thread(new QueryThread(manager, tableName, query, this.quorum, this.port, this.znodeParent));
					thread.start();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (hbaseadmin != null)
				hbaseadmin.close();
		}
	}

	private void convertMap(NavigableMap<HRegionInfo, ServerName> regions, Map<String, HRegionInfo> infos)
	{
		for (HRegionInfo region : regions.keySet()) {
			String tableName = region.getTable().getNameAsString();
			infos.put(((ServerName)regions.get(region)).getServerName() + "@" + tableName, region);
		}
	}

	private void fuzzyMatch(QueryObject query, QueryStatusManager manager)
			throws Exception
	{
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat format1 = new SimpleDateFormat("yyyyMM");

		List<String> scopes = generalScope(query.getStart(), query.getEnd());
		HBaseAdmin hbaseadmin = null;
		HTable table = null;
		try {
			Map<String,HRegionInfo> infos = new HashMap();
			String tableName = "";
			for (String date : scopes)
			{
				String[] datas = date.split("#");
				String start = datas[0];
				Date day = format.parse(start);

				String tn = this.tablePrefix + format1.format(day);
				tableName = tableName + tn + ",";

				hbaseadmin = new HBaseAdmin(conf);

				if (!hbaseadmin.tableExists(tn)) {
					LOG.info(tableName + "表未找到");
					manager.setStatus(tableName, Boolean.valueOf(true));
				} else {
					table = new HTable(conf, tn);

					NavigableMap regions = table.getRegionLocations();
					convertMap(regions, infos);
				}
			}

			for (String serverName : infos.keySet())
			{
				manager.setStatus(serverName, Boolean.valueOf(false));
				HRegionInfo region = (HRegionInfo)infos.get(serverName);
				String tn = region.getTable().getNameAsString();

				Thread thread = new Thread(new CoprocessorQueryThread(tn, conf, manager, tableName, query, serverName));
				thread.start();
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (hbaseadmin != null)
				hbaseadmin.close();
		}
	}

	public TableRecord getRecordForTable(String tableName)
			throws IOException
	{
		HTableInterface table = null;
		try
		{
			table = new HTable(conf, "FSN_TOTAL");

			Get get = new Get(Bytes.toBytes(tableName));
			Result result = table.get(get);
			String totalRecord = "0";
			String invalRecord = "0";
			String errorRecord = "0";
			if ((result != null) && (!result.isEmpty())) {
				totalRecord = new String(result.getValue("cf".getBytes(), "totalRecord".getBytes()));
				invalRecord = new String(result.getValue("cf".getBytes(), "invalRecord".getBytes()));
				errorRecord = new String(result.getValue("cf".getBytes(), "errorRecord".getBytes()));
			}
			TableRecord record = new TableRecord();
			record.setErrorRecord(errorRecord);
			record.setTotalRecord(totalRecord);
			record.setInvalRecord(invalRecord);
			return record;
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (table != null) {
				table.close();
			}
		}
		return null;
	}

	public List<String> query(QueryObject query)
	{
		String gzh = query.getGzh();
		QueryStatusManager manager = new QueryStatusManager();
		manager.clear();
		try
		{
			if ((gzh != null) && (gzh.length() == 10)) {
				exactMatch(query, manager);
			}
			else
				fuzzyMatch(query, manager);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		while (!manager.isCompleted());
		LOG.info("query completed");
		LOG.info("query total count :" + manager.getResults().size());
		return manager.getResults();
	}

	public static void main(String[] args)
	{
		Query query = new HBaseQuery();
		QueryObject obj = new QueryObject();
		obj.setGzh("ASWN");
		obj.setStart("2014-10-10 00:00:00");
		obj.setEnd("2014-10-10 23:59:59");
		obj.setFr("");
		obj.setQy("");
		obj.setSbbm("");
		obj.setWd("");
		obj.setCzr("");
		List result = query.query(obj);
		System.out.println(result.size());
	}
}