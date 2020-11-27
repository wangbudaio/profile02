package com.wang.ETL;

/**
 * @author 王继昌
 * @create 2020-09-22 17:45
 */
public class ETLUtiles {
    public static String clear(String line){
        String[] split = line.split("\t");
        if (split.length<9) {
            return  null ;
        }
        StringBuffer stringBuffer = new StringBuffer();
        for (int i = 0; i < split.length; i++) {
            if (i < 9) {
                if (i+1 == split.length) {
                    stringBuffer.append(split[i]);
                }else{
                    if (i == 3) {
                        split[3].replaceAll(" ","");
                    }
                    stringBuffer.append(split[i]).append("\t");
                }
            }else{
                if (i+1 == split.length) {
                    stringBuffer.append(split[i]);
                }else{
                    stringBuffer.append(split[i]).append("&");
                }
            }
        }
        return stringBuffer.toString();
    }
}
