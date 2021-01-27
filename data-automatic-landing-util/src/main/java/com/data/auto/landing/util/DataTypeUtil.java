package com.data.auto.landing.util;

import java.util.Scanner;
import java.util.regex.Pattern;

public class DataTypeUtil {
    
    public static final String CHAR_PATTERN = "[^0-9]";
    public static Pattern pattern = Pattern.compile("[0-9]*(\\.?)[0-9]*");
    public static Pattern INT_PATTERN = Pattern.compile("^-?[1-9]\\d*$");
    public static Pattern DOUBLE_PATTERN = Pattern.compile("^[-]?[1-9]\\d*\\.\\d*|-0\\.\\d*[1-9]\\d*$");
    
    public static String getDataType(String args) {
        String type = "";
        if ("true".equalsIgnoreCase(args) || "false".equalsIgnoreCase(args)) {
            type = "boolean";
            System.out.print("boolean");
        } else if (INT_PATTERN.matcher(args).matches()) {
            type = "int";
            System.out.print("int");
        } else if (DOUBLE_PATTERN.matcher(args).matches()) {
            type = "double";
            System.out.print("double");
        } else {
            type = "String";
            System.out.print("String");
        }
        return type;
    }
    
    public static void main(String[] args) {
        Scanner cin = new Scanner(System.in);
        String str = cin.nextLine();
        String str1[] = str.split(" ");
        for (int i = 0; i < str1.length; i++) {
            if (i != 0) {
                System.out.print(' ');
            }
            if ("true".equalsIgnoreCase(str1[i]) || "false".equalsIgnoreCase(str1[i])) {
                System.out.print("boolean");
            } else if (INT_PATTERN.matcher(str1[i]).matches()) {
                System.out.print("int");
            } else if (DOUBLE_PATTERN.matcher(str1[i]).matches()) {
                System.out.print("double");
            } else {
                System.out.print("String");
            }
        }
    }
    
    
}
