package com.apache.hbase.bulkimport;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;


/**
 * Created by zhangfeng on 2015/4/16.
 */
public class BytesReaderStream {

    private ByteOrder byteOrder = ByteOrder.BIG_ENDIAN;
    private RandomAccessFile in = null;
    private long length = 0L;

    public BytesReaderStream(String fileName) throws Exception {
        this.length = new File(fileName).length();
        this.in = new RandomAccessFile(fileName, "rw");
    }

    public void setByteOrder(ByteOrder byteOrder)
    {
        this.byteOrder = byteOrder;
    }

    public ByteOrder getByteOrder() {
        return this.byteOrder;
    }

    public void skip(int length) throws Exception {
        this.in.skipBytes(length);
    }
    public void seek(long pos) throws Exception {
        this.in.seek(pos);
    }

    public boolean eof() throws Exception
    {
        return this.in.getFilePointer() >= this.length - 1L;
    }

    public byte[] getBytes(int length) throws Exception {
        byte[] bytes = new byte[length];
        this.in.read(bytes);
        return bytes;
    }

    public byte getByte() throws Exception {
        return getBytes(1)[0];
    }

    public String getString(int length) throws Exception
    {
        byte[] bytes = new byte[length];
        this.in.read(bytes);
        return getStringFromBytes(bytes, 0, length).replaceAll("[^A-Za-z0-9/]*", "");
    }

    public String getStringFromBytes(byte[] bytes, int offset, int length) {
        return new String(bytes, offset, length);
    }

    public int getValFromBytes(byte[] bytes, int offset, int length) {
        int val = 0;
        if (this.byteOrder == ByteOrder.BIG_ENDIAN)
        {
            for (int i = 0; i < length; i++)
            {
                val += ((bytes[(i + offset)] & 0xFF) << i * 8);
            }
        }
        else
        {
            for (int i = length - 1; i >= 0; i--)
            {
                val += ((bytes[(i + offset)] & 0xFF) << i * 8);
            }
        }
        return val;
    }

    public int getInt() throws Exception {
        byte[] bytes = new byte[4];
        this.in.read(bytes);
        return getValFromBytes(bytes, 0, bytes.length);
    }

    public int getUnsignedShort() throws Exception {
        byte[] bytes = new byte[2];
        this.in.read(bytes);
        return getValFromBytes(bytes, 0, bytes.length);
    }

    public void setBytes(byte[] bytes) throws Exception {
        this.in.write(bytes);
    }

    public void close() throws Exception {
        if (this.in != null)
        {
            this.in.close();
        }
    }

}
