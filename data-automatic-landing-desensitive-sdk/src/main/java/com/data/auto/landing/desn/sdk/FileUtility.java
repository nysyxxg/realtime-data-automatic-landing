package com.data.auto.landing.desn.sdk;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public class FileUtility {

	public static FileUtility getFileIt(String fileName) {
		return new FileUtility(fileName);
	}
	
	public FileUtility(String fileName) {
		file = new File(fileName);
		try {
			read = new InputStreamReader( new FileInputStream(file));
			bufferedReader = new BufferedReader(read);
		} catch (Exception e) {
		}
	}
	
	private File file = null;
	private InputStreamReader read = null;
	private BufferedReader bufferedReader = null;
	private String lineTxt = null;
	
	public String getLine() {
		try {
			lineTxt = bufferedReader.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
		if(lineTxt == null) {
			try {
				bufferedReader.close();
				read.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return lineTxt;
	}
	
	public int getBytes(int size) {
		int readByte = -1;
		char [] cbuf = new char[1024];
		try {
			readByte = bufferedReader.read(cbuf);
		} catch (IOException e) {
			e.printStackTrace();
		}
		if(readByte == -1) {
			try {
				bufferedReader.close();
				read.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return readByte;
	}
	
	public String getAll() {
		StringBuilder r = new StringBuilder();
		try {
			while(true) {
				lineTxt = bufferedReader.readLine();
				if(lineTxt == null) {
					try {
						bufferedReader.close();
						read.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
					break;
				} else {
					r.append(lineTxt);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return r.toString();
	}
	
}
