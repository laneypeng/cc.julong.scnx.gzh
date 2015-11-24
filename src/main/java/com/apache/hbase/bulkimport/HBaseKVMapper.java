package com.apache.hbase.bulkimport;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HBaseKVMapper extends Mapper<Text, BytesWritable, ImmutableBytesWritable, KeyValue> {
	static final byte[] SRV_COL_FAM = "cf".getBytes();
	static final int NUM_FIELDS = 11;
	private static final int recordLength = 100;
	private static final int FSN_IMAGE_LENGTH = 1544;
	ImmutableBytesWritable hKey = new ImmutableBytesWritable();
	
	protected void setup(Context context)
			throws IOException, InterruptedException {
		Configuration c = context.getConfiguration();
	}

	protected void map(Text key,BytesWritable value,Context context)
			throws IOException, InterruptedException {
		String[] fields = null;
		byte[] fsnBuffer = value.getBytes();

		ByteBuffer byteBuffer = ByteBuffer.wrap(fsnBuffer);

		byte[] bf = new byte[recordLength];
		byteBuffer.get(bf, 0, recordLength);

		byte[] fsnImageByte = new byte[FSN_IMAGE_LENGTH];
		byteBuffer.get(fsnImageByte, 0, FSN_IMAGE_LENGTH);

		FSNDecoder decoder = new FSNDecoder(bf);
		String recordValue = decoder.getRecord();
		try {

			fields = recordValue.split(",");
		} catch (Exception ex) {
			context.getCounter(Driver.MY_COUNTER.PARSE_ERRORS).increment(1L);
			return;
		}

		if (fields.length != NUM_FIELDS) {
			context.getCounter(Driver.MY_COUNTER.INVALID_FIELD_LEN).increment(1L);
			return;
		}
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		try {
			long dateTime = format.parse(fields[0]).getTime() / 1000L;

			String rowkey = fields[7] + dateTime;
			
			String[] keys = key.toString().split("#");
			//解析文件名称获取区域、法人网点信息：
			//文件名格式是：区域_法人_网点_时间（精确到秒）.zip 示例：11_22_001_20150612132201.zip
			String[] names = keys[2].split("_");
			
			String[] values = recordValue.split(",");
			StringBuffer buffer = new StringBuffer();
			//区域_法人_网点_时间（精确到秒）.FSN
			buffer.append(dateTime).append(",");//1.时间
			buffer.append(names[0]).append(",");//2. 区域 （字符串2位）
			buffer.append(names[1]).append(",");//3. 法人  （字符串2位）；
			buffer.append(names[2]).append(",");//4. 网点 (字符串4位)
			buffer.append(values[8].substring(10)).append(",");//5. 设备编码（字符串6位）新增；
			buffer.append(values[9]).append(",");//6. 操作人（字符串5位）新增；
			buffer.append(values[1]).append(",");//7. 纸币类型 （数字2位）；
			buffer.append(values[2]).append(",");//8. 纸币信息（字符串5位）
			buffer.append(values[3]).append(",");//9.币种（字符串3位）
			buffer.append(values[10]).append(",");//10.版本 （字符串4位）
			buffer.append(values[5]).append(",");//11.面值 （数字4位）
			buffer.append(values[7]).append(",");//12.冠字号 （字符串10位）；
			buffer.append(values[8]).append(",");//13 设备号 （字符串24位）；
			buffer.append(keys[0]).append(",");//14 文件名称 （字符串50位）；
			buffer.append(keys[1]);//15 偏移量 数字（8位）
			
			this.hKey.set(rowkey.getBytes());

			KeyValue kv = new KeyValue(this.hKey.get(), SRV_COL_FAM,HColumnEnum.SRV_COL_B.getColumnName(),buffer.toString().getBytes());
			// KeyValue fsn_kv_image = new KeyValue(this.hKey.get(),
			// SRV_COL_FAM, HColumnEnum.SRV_COL_C.getColumnName(),
			// fsnImageByte);

			context.write(this.hKey, kv);
			// context.write(this.hKey, fsn_kv_image);

			context.getCounter(Driver.MY_COUNTER.NUM_MSGS).increment(1L);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}