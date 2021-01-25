package com.data.auto.landing.sdk;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class ByteUtil {
	
	/**
	 * 字节数组转字符数组
	 * @param bytes
	 * @return
	 */
    public static char[] bytesToChars(byte[] bytes) {
        Charset cs = Charset.forName("UTF-8");
        ByteBuffer bb = ByteBuffer.allocate(bytes.length);
        bb.put(bytes);
        bb.flip();
        CharBuffer cb = cs.decode(bb);
        return cb.array();
    }
    
    /**
     * 字符数组转字节数组
     * @param chars
     * @return
     */
    public static byte[] charsToBytes(char[] chars) {
        Charset cs = Charset.forName("UTF-8");
        CharBuffer cb = CharBuffer.allocate(chars.length);
        cb.put(chars);
        cb.flip();
        ByteBuffer bb = cs.encode(cb);
        return bb.array();
    }
    
    /**
     * 字节数组转Int
     * @param bytes
     * @return
     */
    public static int bytesToInt(byte[] bytes, int offset) {
		int result = 0;
		//将每个byte依次搬运到int相应的位置
		result = bytes[offset] & 0xff;
		result = result << 8 | bytes[offset + 1] & 0xff;
		result = result << 8 | bytes[offset + 2] & 0xff;
		result = result << 8 | bytes[offset + 3] & 0xff;
		return result;
	}
    
    public static int bytesToInt(byte[] bytes) {
		return bytesToInt(bytes, 0);
	}
    
	public static short bytesToShort(byte[] bytes, int offset) {
		short sRet = 0;
	    sRet += (bytes[offset] & 0xFF) << 8;
	    sRet += bytes[offset + 1] & 0xFF;
	    return sRet;
	}
	
    public static int bytesToShort(byte[] bytes) {
		return bytesToShort(bytes, 0);
	}
	
    /**
     * Int转字节数组
     * @param num
     * @return
     */
	public static byte[] intToBytes(int num) {
		byte[] bytes = new byte[4];
		//通过移位运算，截取低8位的方式，将int保存到byte数组
		bytes[0] = (byte)(num >>> 24);
		bytes[1] = (byte)(num >>> 16);
		bytes[2] = (byte)(num >>> 8);
		bytes[3] = (byte)num;
		return bytes;
	}
	
	public static byte[] shortToBytes(short num) {
		byte[] bytes = new byte[2];
		//通过移位运算，截取低8位的方式，将int保存到byte数组
		bytes[0] = (byte)(num >>> 8);
		bytes[1] = (byte)num;
		return bytes;
	}
	

    /**将二进制转换成16进制 
     * @param buf 
     * @return 
     */  
    public static String bytesToHexString(byte buf[]) {  
            StringBuffer sb = new StringBuffer();  
            for (int i = 0; i < buf.length; i++) {  
                    String hex = Integer.toHexString(buf[i] & 0xFF);  
                    if (hex.length() == 1) {  
                            hex = '0' + hex;  
                    }
                    sb.append(hex.toUpperCase());  
            }  
            return sb.toString();  
    } 
    
    /**将16进制转换为二进制 
     * @param hexStr 
     * @return
     */  
    public static byte[] hexStringToBytes(String hexStr) {  
            if (hexStr.length() < 1)
                    return null;  
            byte[] result = new byte[hexStr.length()/2];  
            for (int i = 0;i< hexStr.length()/2; i++) {  
                    int high = Integer.parseInt(hexStr.substring(i*2, i*2+1), 16);  
                    int low = Integer.parseInt(hexStr.substring(i*2+1, i*2+2), 16);  
                    result[i] = (byte) (high * 16 + low);  
            }  
            return result;  
    }
    
    /**
     * 比较两个字节数组是否相等
     * @param a
     * @param b
     * @return
     */
    public static boolean bytesEquals(byte [] a, byte [] b, int b_offset, int b_length) {
    	boolean r = true;
    	if(a.length == b_length - b_offset) {
    		for(int i = 0;i < a.length;i++) {
    			if(a[i] != b[i]) {
    				r = false;
    				break;
    			}
    		}
    	}
    	return r;
    }
    
    public static boolean bytesEquals(byte [] a, byte [] b) {
    	return bytesEquals(a,b,0,b.length);
    }
    
    /**
     * 调试Bytes用
     * @param a
     * @return
     */
    public static List<Integer> printBytes(byte [] a) {
    	List<Integer> b = new ArrayList<Integer>();
    	for(int i = 0; i < a.length ; i++ ) {
    		b.add((int)a[i]);
    	}
    	return b;
    }
}
