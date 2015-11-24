package com.apache.hbase.bulkimport;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.nio.ByteOrder;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * FSN文件解码类
 * 
 * @author zhangfeng
 *
 */
public class FSNDecoder {

	private static ByteOrder byteOrder = ByteOrder.BIG_ENDIAN;

	private RandomAccessFile in = null;

	private long fileLength = 0l;

	private byte[] buffer = new byte[100];

    private byte[] fsn_buffer = new byte[1544];

	private int recordCount = 0;
	
	private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public FSNDecoder() {
		try {
			this.in = new RandomAccessFile("D:\\sp_20150731_155235.fsn","r");
			this.fileLength = this.in.length();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	
	public FSNDecoder(byte[] buffer){
		this.buffer = buffer;
	}
	
	
	public static void main(String args[]) {
		try {
            int index = 0;
			FSNDecoder decoder = new FSNDecoder();
			SimpleDateFormat format = new SimpleDateFormat(
					"yyyy-MM-dd HH:mm:ss");
			int i = 0;
			while (decoder.read()) {
				StringBuffer buffer = new StringBuffer();
				buffer.append(format.format(decoder.getFsnDatetime())).append(",");// 验钞启动日期
				buffer.append(decoder.getFsnTfFlag()).append(",");// 纸币类型
				int[] errorCode = decoder.getFsnErrorCode();
				for (int code : errorCode) {
					buffer.append(code).append(" "); // 错误码(3个)
				}
				buffer.append(",");
				buffer.append(decoder.getFsnMoneyFlag()).append(",");// 货币标志
				buffer.append(decoder.getFsnVer()).append(",");// 版本号
				buffer.append(decoder.getFsnValuta()).append(",");// 币值
				buffer.append(decoder.getFsnCharNUM()).append(",");// 冠字号码字符数
				buffer.append(decoder.getFsnSno()).append(",");// 冠字号码
				buffer.append(decoder.getFsnMachineSno()).append(",");// 机具编号
				buffer.append(decoder.getFsnReserve()).append(","); // 操作员

				buffer.append(decoder.getFsnVer()).append(",");
                buffer.append(index);
                index += 1644;
				System.out.println(buffer.toString());
                break;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

    public byte[] getFsnImage(){
        return this.fsn_buffer;
    }

    /**
     * 1. 时间（秒数）；
     *		2. 区域 （字符串2位）；
     *		3. 法人  （字符串2位）；
     *		4. 网点 (字符串4位)；
     *		5. 设备编码（字符串6位）新增；
     *		6. 操作人（字符串5位）新增；
     *		7. 纸币类型 （数字2位）；
     *		8. 纸币信息（字符串5位）；
     *		9.币种（字符串3位）
     *		10.版本 （字符串4位）
     *		11.面值 （数字4位）；
     *		12.冠字号 （字符串10位）；
     *		13 设备号 （字符串24位）；
     *		14 文件名称 （字符串50位）；
     *		15 偏移量 数字（8位）
     * @return
     */
    public String getRecord(){

		StringBuffer buffer = new StringBuffer();
		buffer.append(this.format.format(this.getFsnDatetime())).append(",");// 验钞启动日期
		//buffer.append(this.get).append(",");
		buffer.append(this.getFsnTfFlag()).append(",");// 纸币类型
		int[] errorCode = this.getFsnErrorCode();
		for (int code : errorCode) {
			buffer.append(code).append(";"); // 错误码(3个)
		}
		buffer.append(",");
		buffer.append(this.getFsnMoneyFlag()).append(",");// 货币标志
		buffer.append(this.getFsnVer()).append(",");// 版本号
		buffer.append(this.getFsnValuta()).append(",");// 币值
		buffer.append(this.getFsnCharNUM()).append(",");// 冠字号码字符数
		buffer.append(this.getFsnSno()).append(",");// 冠字号码
		buffer.append(this.getFsnMachineSno()).append(",");// 机具编号
		buffer.append(this.getFsnReserve()).append(","); // 操作员

		buffer.append(this.getFsnVer());
		return buffer.toString();
	}
	
	/**
	 * 读取一条记录
	 * @return
	 * @throws Exception
	 */
	public boolean read() throws Exception {
		if (this.eof()) {
			return false;
		}
		if (recordCount <= 0) {
			recordCount = this.getRecordCounter();
		}
		this.in.read(this.buffer);
        this.in.read(this.fsn_buffer);
		return true;
	}

	/**
	 * 文件是否读完
	 * 
	 * @return
	 * @throws Exception
	 */
	public boolean eof() throws Exception {
		return this.in.getFilePointer() >= this.fileLength - 1L;
	}

	/**
	 * 版本号
	 * 
	 * @return
	 */
	public String getFsnVer() {
		int ver = this.getValFromBytes(buffer, 20, 2);
		switch (ver) {

		case 2:
			return "2005";
		case 1:
			return "1999";
		case 0:
			return "1990";
		case 3 :
			return "2015";
		default:
			return String.valueOf(ver);
		}
	}

	/**
	 * 币值
	 * @return
	 */
	public int getFsnValuta() {
		return this.getValFromBytes(buffer, 22, 2);
	}


    private byte[] getFSNImage(){
        return this.fsn_buffer;
    }

	/**
	 * 冠字号码字符数
	 * @return
	 */
	public int getFsnCharNUM() {
		return this.getValFromBytes(buffer, 24, 2);
	}

	/**
	 * 冠字号码
	 * @return
	 */
	public String getFsnSno() {
		return this.getStringFromBytes(buffer, 26, 24).replaceAll(
				"[^A-Za-z0-9/]*", "");
	}

	/**
	 * 机具编号
	 * @return
	 */
	public String getFsnMachineSno() {
		return this.getStringFromBytes(buffer, 50, 48).replaceAll(
				"[^A-Za-z0-9/]*", "");
	}

	/**
	 * 操作员
	 *
	 * @return
	 */
	public int getFsnReserve() {
		return this.getValFromBytes(buffer, 98, 2);
	}

	/**
	 * 货币标志
	 * @return
	 */
	public String getFsnMoneyFlag() {
		return this.getStringFromBytes(buffer, 12, 8).replaceAll(
				"[^A-Za-z0-9/]*", "");
	}

	/**
	 * 获取错误码(3个)
	 * 
	 * @return
	 */
	public int[] getFsnErrorCode() {
		int[] arr = new int[3];
		for (int i = 0; i < arr.length; i++) {
			arr[i] = this.getValFromBytes(buffer, 6 + i, 2);
		}
		return arr;
	}

	/**
	 * 获取纸币类型
	 * @return
	 */
	public int getFsnTfFlag() {
		return this.getValFromBytes(buffer, 4, 2);
	}

	/**
	 * 获取时间
	 * @return
	 */
	public Date getFsnDatetime() {
		int val = getValFromBytes(buffer, 0, 2);
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.YEAR, 1980 + ((val >> 9) & 0x7F));
		cal.set(Calendar.MONTH, ((val >> 5) & 0xF) - 1);
		cal.set(Calendar.DAY_OF_MONTH, (val & 0x1F));
		val = getValFromBytes(buffer, 2, 2);
		cal.set(Calendar.HOUR_OF_DAY, ((val >> 11) & 0x1F));
		cal.set(Calendar.MINUTE, ((val >> 5) & 0x3F));
		cal.set(Calendar.SECOND, (val & 0x1F) << 1);
		return cal.getTime();
	}

	public int getValFromBytes(byte[] bytes, int offset, int length) {
		int val = 0;
		if (byteOrder == ByteOrder.BIG_ENDIAN) {
			for (int i = 0; i < length; i++) {
				val += ((bytes[(i + offset)] & 0xFF) << i * 8);
			}
		} else {
			for (int i = length - 1; i >= 0; i--) {
				val += ((bytes[(i + offset)] & 0xFF) << i * 8);
			}
		}
		return val;
	}

	public String getString(int length) throws Exception {
		byte[] bytes = new byte[length];
		this.in.read(bytes);
		return getStringFromBytes(bytes, 0, length).replaceAll(
				"[^A-Za-z0-9/]*", "");
	}

	public String getStringFromBytes(byte[] bytes, int offset, int length) {
		return new String(bytes, offset, length);
	}


	public int getRecordCount(){
		return recordCount;
	}
	/**
	 * 读取记录总数
	 * 
	 * @return
	 */
	private int getRecordCounter() {
		try {
			this.in.seek(0);
			this.in.skipBytes(20);
			int count = getInt();
			in.skipBytes(8);
			return count;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}

	public int getInt() throws Exception {
		byte[] bytes = new byte[4];
		this.in.read(bytes);
		return getValFromBytes(bytes, 0, bytes.length);
	}
}
