package com.data.auto.landing.desn.sdk;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

/** 
 *AES加密解密工具类 
 *@author M-Y 
 */  
public class AESUtil {
	/**
     * AES加密字符串
     * 
     * @param content
     *            需要被加密的字符串
     * @param password
     *            加密需要的密码
     * @return 密文
     */
    public static byte[] encrypt(byte [] content, String password) {
        try {
            KeyGenerator kgen = KeyGenerator.getInstance("AES");// 创建AES的Key生产者

            SecureRandom random = SecureRandom.getInstance("SHA1PRNG");
            random.setSeed(password.getBytes());
            kgen.init(128, random);
            //加密没关系，SecureRandom是生成安全随机数序列，password.getBytes()是种子，只要种子相同，序列就一样，所以解密只要有password就行

            SecretKey secretKey = kgen.generateKey();// 根据用户密码，生成一个密钥

            byte[] enCodeFormat = secretKey.getEncoded();// 返回基本编码格式的密钥，如果此密钥不支持编码，则返回
                                                            // null。

            SecretKeySpec key = new SecretKeySpec(enCodeFormat, "AES");// 转换为AES专用密钥

            Cipher cipher = Cipher.getInstance("AES");// 创建密码器

            byte[] byteContent = content;

            cipher.init(Cipher.ENCRYPT_MODE, key);// 初始化为加密模式的密码器

            byte[] result = cipher.doFinal(byteContent);// 加密

            return result;

        } catch ( Exception e) {
            e.printStackTrace();
        }
        return null;
    }
    /**
     * 解密AES加密过的字符串
     * 
     * @param content
     *            AES加密过过的内容
     * @param password
     *            加密时的密码
     * @return 明文
     */
    public static byte[] decrypt(byte[] content, String password) {
        try {
            KeyGenerator kgen = KeyGenerator.getInstance("AES");// 创建AES的Key生产者
            
            SecureRandom random = SecureRandom.getInstance("SHA1PRNG");
            random.setSeed(password.getBytes());
            kgen.init(128, random);
            
            SecretKey secretKey = kgen.generateKey();// 根据用户密码，生成一个密钥
            byte[] enCodeFormat = secretKey.getEncoded();// 返回基本编码格式的密钥
            SecretKeySpec key = new SecretKeySpec(enCodeFormat, "AES");// 转换为AES专用密钥
            Cipher cipher = Cipher.getInstance("AES");// 创建密码器
            cipher.init(Cipher.DECRYPT_MODE, key);// 初始化为解密模式的密码器
            byte[] result = cipher.doFinal(content);  
            return result; // 明文   
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
    

    
    public static String encryptStr(String content, String password) {
		return ByteUtil.bytesToHexString(encrypt(content.getBytes(StandardCharsets.UTF_8), password));
    }
    
    public static String decryptStr(String content, String password) {
    	return new String(decrypt(ByteUtil.hexStringToBytes(content), password), StandardCharsets.UTF_8);
    }
    

    
    /**
     * 解密流
     * @param is
     * @param os
     * @param password
     * @throws IOException
     */
    public static void decryptStream(InputStream is , OutputStream os, String password) throws IOException {
    	int dataOfFile = 0;
    	byte [] buf = new byte[1024 * 1024];  
    	while ((dataOfFile = is.read(buf)) > -1) {
    		for(int i = 0; i < dataOfFile; i++) {
    			buf[i] = (byte) (buf[i] ^ Math.abs(password.hashCode()));
    		}
    		os.write(buf, 0, dataOfFile);
    	}
    	is.close();
    	os.flush();
    	os.close();
    }
    /**
     * 解密流Buffer
     * @param password
     * @throws IOException
     */
	public static void decryptStream(byte[] buf, int len, String password) throws IOException {
		for (int i = 0; i < len; i++) {
			buf[i] = (byte) (buf[i] ^ Math.abs(password.hashCode()));
		}
	}
    /**
     * 加密流Buffer
     * @param password
     * @throws IOException
     */
    public static void encryptStream(byte[] buf, int len, String password) throws IOException {
    	decryptStream(buf, len, password);
    }
    /**
     * 加密流
     * @param is
     * @param os
     * @param password
     * @throws IOException
     */
    public static void encryptStream(InputStream is , OutputStream os, String password) throws IOException {
    	decryptStream(is, os, password);
    }
    
    public static void main(String[] args) throws Exception {
    	String content = "hello";
        String password = "123";
        String code = encryptStr(content, password);
        System.out.println(code);
        System.out.println(decryptStr("3206FDD7EC8E596524BD136556F24C14", password));
        System.out.println("String大小（原来/加密后）: " + content.length() + "/" + code.length());
        System.out.println("Bytes大小（原来/加密后）: " + content.getBytes().length + "/" + encrypt(content.getBytes(), password).length);
        //将磁盘上的文件加密后并解密
        try {
        	//文件加密
        	File srcFile = new File("/Users/wenyiqiu/Downloads/name.txt");//原文件等待被加密
            File encryptedFile = new File("/Users/wenyiqiu/Downloads/name.txt.enc");//加密后的文件
            if(!encryptedFile.exists()){
            	System.out.println("encrypt file created");
            	encryptedFile.createNewFile();
            }
            String filePwd = "xxx";//文件加密密码
			InputStream srcFileIs  = new FileInputStream(srcFile);
	        OutputStream encryptedFileOS = new FileOutputStream(encryptedFile);
	        encryptStream(srcFileIs, encryptedFileOS, filePwd);//完成文件加密过程
	        
	        //文件解密
	        File decryptedFile = new File("/Users/wenyiqiu/Downloads/name.txt.dec.txt");
	        InputStream encryptedFileIs  = new FileInputStream(encryptedFile);
	        OutputStream decryptedFileOS = new FileOutputStream(decryptedFile);
            if(!decryptedFile.exists()){
            	System.out.println("decrypt file created");
            	decryptedFile.createNewFile();
            }
            decryptStream(encryptedFileIs, decryptedFileOS, filePwd);//完成文件解密过程

		} catch (Exception e) {
			e.printStackTrace();
		}
        
    }
}  
