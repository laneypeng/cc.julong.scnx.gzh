package com.apache.hbase.bulkimport;

import java.awt.Graphics;
import java.awt.Image;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.awt.image.Raster;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import javax.imageio.ImageIO;


/**
 * Created by zhangfeng on 2015/4/16.
 */
public class FsnImageHelper {
    private final int PALETTE_SIZE = 8;
    private final int BITMAPFILEHEADER_SIZE = 14;
    private final int BITMAPINFOHEADER_SIZE = 40;
    private final int FSN_IMAGE_WIDTH = 320;
    private final int FSN_IMAGE_HEIGHT = 32;
    public int SNO_NUM = 10;
    private int SNO_MAX = 12;
    private int destOffset = 0;
    private byte[] destArr = null;
    private BytesReaderStream brStream = null;

    public FsnImageHelper(String fsnFileFullName)
    {
        if ((fsnFileFullName == null) || (fsnFileFullName.isEmpty()))
        {
            System.out.println("fsn file name is empty!!");
            return;
        }
        File f = new File(fsnFileFullName);
        if (!f.exists())
        {
            System.out.println(fsnFileFullName + " does not exist!!");
            return;
        }
        try {
            this.brStream = new BytesReaderStream(fsnFileFullName);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
    public FsnImageHelper() {
    }

    public void BytesCopy(byte[] sourceArr) { System.arraycopy(sourceArr, 0, this.destArr, this.destOffset, sourceArr.length);
        this.destOffset += sourceArr.length; }

    public byte[] getFsnImageBytes(byte[] fsnFileBytes, int offset)
            throws Exception
    {
        if (fsnFileBytes.length - offset < 1544)
        {
            throw new Exception("the length should be greater or equal to 1544 from the offset");
        }
        byte[] bytes = new byte[1544];
        System.arraycopy(fsnFileBytes, offset, fsnFileBytes, 0, 1544);
        return getFsnImageBytes(bytes);
    }

    public byte[] getFsnImageBytes(byte[] fsnFileBytes)
    {
        byte[] fsnBytes = GetBmpRawData(fsnFileBytes, 0, Boolean.valueOf(false));
        return getMonochromeBitmapBytes(fsnBytes);
    }

    public byte[] getMonochromeBitmapBytes(byte[] monochromeRawBytes) {
        this.destArr = new byte[monochromeRawBytes.length + 8 + 14 + 40];

        byte[] paletteBytes = { -1, -1, -1 };

        byte[] bfType = { 66, 77 };
        int bfSize = this.destArr.length;
        int bfReserved1 = 0;
        int bfReserved2 = 0;
        int bfOffBits = 62;

        int biSize = 40;
        int biWidth = this.SNO_NUM * 32;
        int biHeight = 32;
        int biPlanes = 1;
        int biBitCount = 1;
        int biCompression = 0;
        int biSizeImage = (biWidth * biBitCount + 31) / 32 * 4;
        int biXPelsPerMeter = 0;
        int biYPelsPerMeter = 0;
        int biClrUsed = 0;
        int biClrImportant = 0;

        this.destOffset = 0;

        BytesCopy(bfType);

        BytesCopy(intToDWord(bfSize));
        BytesCopy(intToWord(bfReserved1));
        BytesCopy(intToWord(bfReserved2));
        BytesCopy(intToDWord(bfOffBits));

        BytesCopy(intToDWord(biSize));
        BytesCopy(intToDWord(biWidth));
        BytesCopy(intToDWord(biHeight));
        BytesCopy(intToWord(biPlanes));
        BytesCopy(intToWord(biBitCount));
        BytesCopy(intToDWord(biCompression));
        BytesCopy(intToDWord(biSizeImage));
        BytesCopy(intToDWord(biXPelsPerMeter));
        BytesCopy(intToDWord(biYPelsPerMeter));
        BytesCopy(intToDWord(biClrUsed));
        BytesCopy(intToDWord(biClrImportant));

        BytesCopy(paletteBytes);
        BytesCopy(monochromeRawBytes);
        return this.destArr;
    }

    public byte[] getFsnImageRawBytes(int offset, int length) throws Exception
    {
        if (length < 1544)
        {
            throw new Exception("the length should be greater or equal to 1544 ");
        }

        byte[] bytes = getFsnImageRawBytes(offset);
        byte[] destBytes = new byte[length];
        System.arraycopy(bytes, 0, destBytes, 0, bytes.length);
        return destBytes;
    }

    public byte[] getFsnImageRawBytes(int offset) throws Exception
    {
        this.brStream.seek(offset);
        return this.brStream.getBytes(1544);
    }

    public byte[] getFsnImageBytes(int offset) {
        byte[] imageBytes = new byte[0];
        if (this.brStream == null)
        {
            return imageBytes;
        }
        try {
            byte[] bytes = getFsnImageRawBytes(offset);

            imageBytes = getFsnImageBytes(bytes);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return imageBytes;
    }

    public void close() {
        try {
            if (this.brStream != null)
            {
                this.brStream.close();
            }

        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public void saveToFile(byte[] fsnImageData, int offset, String bmpFileName) {
        try {
            byte[] bmpRawData = GetBmpRawData(fsnImageData, offset, Boolean.valueOf(false));

            byte[] paletteBytes = { -1, -1, -1 };

            byte[] bfType = { 66, 77 };
            int bfSize = paletteBytes.length + bmpRawData.length + 14 + 40;
            int bfReserved1 = 0;
            int bfReserved2 = 0;
            int bfOffBits = 54;

            int biSize = 40;
            int biWidth = this.SNO_NUM * 32;
            int biHeight = 32;
            int biPlanes = 1;
            int biBitCount = 1;
            int biCompression = 0;
            int biSizeImage = bmpRawData.length;
            int biXPelsPerMeter = 0;
            int biYPelsPerMeter = 0;
            int biClrUsed = 0;
            int biClrImportant = 0;
            FileOutputStream fo = new FileOutputStream(bmpFileName);

            fo.write(bfType);
            fo.write(intToDWord(bfSize));
            fo.write(intToWord(bfReserved1));
            fo.write(intToWord(bfReserved2));
            fo.write(intToDWord(bfOffBits));

            fo.write(intToDWord(biSize));
            fo.write(intToDWord(biWidth));
            fo.write(intToDWord(biHeight));
            fo.write(intToWord(biPlanes));
            fo.write(intToWord(biBitCount));
            fo.write(intToDWord(biCompression));
            fo.write(intToDWord(biSizeImage));
            fo.write(intToDWord(biXPelsPerMeter));
            fo.write(intToDWord(biYPelsPerMeter));
            fo.write(intToDWord(biClrUsed));
            fo.write(intToDWord(biClrImportant));

            fo.write(paletteBytes);
            fo.write(bmpRawData);
            fo.close();
        }
        catch (Exception localException)
        {
        }
    }

    public static byte[] intToWord(int parValue)
    {
        byte[] retValue = new byte[2];
        retValue[0] = ((byte)(parValue & 0xFF));
        retValue[1] = ((byte)(parValue >> 8 & 0xFF));
        return retValue;
    }

    public static byte[] intToDWord(int parValue)
    {
        byte[] retValue = new byte[4];
        retValue[0] = ((byte)(parValue & 0xFF));
        retValue[1] = ((byte)(parValue >> 8 & 0xFF));
        retValue[2] = ((byte)(parValue >> 16 & 0xFF));
        retValue[3] = ((byte)(parValue >> 24 & 0xFF));
        return retValue;
    }

    private static short byte2int16(byte[] res, int offset)
    {
        return (short)((res[(offset + 1)] << 8) + (res[offset] << 0));
    }

    private byte[] GetBmpRawData(byte[] recordData, int offset, Boolean isRevert) {
        this.SNO_NUM = byte2int16(recordData, offset);
        if ((this.SNO_NUM < 0) || (this.SNO_NUM > 12))
        {
            this.SNO_NUM = 10;
        }
        int sNoHeight = 32;
        int sNoWidth = 32;

        int bmpWidth = sNoWidth * this.SNO_NUM;
        int byteWidth = bmpWidth / 8;
        byte[] bmpRawData = new byte[byteWidth * sNoHeight];

        for (int col = 0; col < this.SNO_NUM * sNoWidth; col++)
        {
            for (int row = 0; row < sNoHeight; row++)
            {
                byte pixelByte = (byte)((recordData[(8 + col * 4 + row / 8)] >> row % 8 & 0x1) << 7 - col % 8);

                if (isRevert.booleanValue())
                {
                    int tmp136_135 = ((sNoHeight - row - 1) * byteWidth + col / 8);
                    byte[] tmp136_118 = bmpRawData; tmp136_118[tmp136_135] = ((byte)(tmp136_118[tmp136_135] + pixelByte));
                }
                else
                {
                    int tmp159_158 = (row * byteWidth + col / 8);
                    byte[] tmp159_146 = bmpRawData; tmp159_146[tmp159_158] = ((byte)(tmp159_146[tmp159_158] + pixelByte));
                }
            }
        }
        if (bmpRawData.length == 0)
        {
            for (int i = 0; i < recordData.length; i++)
            {
                recordData[i] = -1;
            }
            return recordData;
        }

        return bmpRawData;
    }

    public static byte[] hex2bytes(String hexString) {
        String[] arr = hexString.split(" ");
        byte[] bytes = new byte[arr.length];
        for (int i = 0; i < arr.length; i++)
        {
            bytes[i] = ((byte)Integer.parseInt(arr[i], 16));
        }
        return bytes;
    }
    public static String bytes2hex(byte[] bytes) {
        StringBuffer sBuffer = new StringBuffer();

        for (int i = 0; i < bytes.length; i++)
        {
            int v = bytes[i] & 0xFF;
            String hv = Integer.toHexString(v);
            if (hv.length() < 2)
            {
                sBuffer.append(0);
            }
            sBuffer.append(hv).append(" ");
        }
        return sBuffer.toString();
    }
    private byte[] GetMonochromeData(int bmp24Width, int bmp24Height, byte[] bmp24Data) {
        byte pixelByte = 0;
        int outStride = bmp24Width / 8;
        int inStride = bmp24Width * 3;
        byte[] outMonoBytes = new byte[outStride * bmp24Height];
        int rgb = 0;
        for (int y = 0; y < bmp24Height; y++) {
            for (int x = 0; x < bmp24Width; x++)
            {
                int b = bmp24Data[(y * inStride + x * 3 + 2)] & 0xFF;
                int g = bmp24Data[(y * inStride + x * 3 + 1)] & 0xFF;
                int r = bmp24Data[(y * inStride + x * 3)] & 0xFF;
                rgb += (int)(0.2125D * r + 0.7154D * g + 0.0721D * b);
            }
        }

        int grayValue = (int)(rgb / (bmp24Height * bmp24Width) * 0.9D);
        for (int y = 0; y < bmp24Height; y++) {
            for (int x = 0; x < bmp24Width; x++)
            {
                int b = bmp24Data[(y * inStride + x * 3 + 2)] & 0xFF;
                int g = bmp24Data[(y * inStride + x * 3 + 1)] & 0xFF;
                int r = bmp24Data[(y * inStride + x * 3)] & 0xFF;
                rgb = (int)(0.2125D * r + 0.7154D * g + 0.0721D * b);
                pixelByte = (byte)(pixelByte | (byte)((rgb >= grayValue ? 1 : 0) << 7 - x % 8));
                if ((x + 1) % 8 == 0) {
                    outMonoBytes[(y * outStride + x / 8)] = pixelByte;
                    pixelByte = 0;
                }
            }
        }
        return outMonoBytes;
    }

 }
