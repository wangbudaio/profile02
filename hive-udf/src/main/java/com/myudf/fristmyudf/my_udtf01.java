package com.myudf.fristmyudf;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author 王继昌
 * @create 2020-09-21 9:04
 */
public class my_udtf01 extends GenericUDTF {
    List<String> outList  = new ArrayList<>();
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        //约定返回的列的名字
        List<String> fieldNames = new ArrayList<>();
        fieldNames.add("frist");
        fieldNames.add("seconder");
        //约定返回的列的类型
        List<ObjectInspector> fieldOIs = new ArrayList<>();
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getColumnarStructObjectInspector(fieldNames,fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        //需要处理的数据
        String data = args[0].toString();
        //获取传入到函数的第二个参数:  行分隔符
        String linesplit = args[1].toString();
        //获取传入到函数的第三个参数:  列分隔符
        String colsplit = args[2].toString();

        String[] rows = data.split(linesplit);
        for (String row : rows) {
            String[] words = row.split(colsplit);
            outList.clear();
            //将数据一行行写出去
            for (String word : words) {
                outList.add(word);
            }
            forward(outList);
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
