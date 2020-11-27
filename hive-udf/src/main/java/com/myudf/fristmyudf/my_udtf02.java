package com.myudf.fristmyudf;
import com.google.gson.JsonArray;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;

import java.util.ArrayList;
import java.util.List;

/**
 * @author 王继昌
 * @create 2020-09-21 9:04
 */

public class my_udtf02 extends GenericUDTF {

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        //参数个数
        if (argOIs.getAllStructFieldRefs().size() != 1) {
            throw new UDFArgumentLengthException("Input Args Length Error!!!");
        }
        //参数类型
        String typeName = argOIs.getAllStructFieldRefs().get(0).getFieldName();
        if ("string".equals(typeName)) {
            throw new UDFArgumentTypeException(0,"explode_json_array函数的第1个参数的类型只能为String...");
        }
        //函数的返回值类型
        ArrayList<String> fieldNames  = new ArrayList<>();
        fieldNames.add("aaa");
        ArrayList<ObjectInspector> fieldOIs  = new ArrayList<>();
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        //1.获取jsonArrayStr
        String jsonArrayStr = args[0].toString();
        //2.将jsonArrayStr转换成JSONArray
        JSONArray jsonArrays = new JSONArray(jsonArrayStr);
        //输出
        for (int i = 0; i < jsonArrays.length(); i++) {
            String action = jsonArrays.getString(i);
            String[] strings = new String[1];
            strings[0] = action;
            forward(strings);
        }


    }

    @Override
    public void close() throws HiveException {

    }
}
