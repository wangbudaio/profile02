package com.myudf.fristmyudf;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * @author 王继昌
 * @create 2020-09-19 16:32
 */
public class my_udf01 extends GenericUDF {
    /*
    * 用于初始化
    * */
    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length != 1) {
            throw new UDFArgumentException("input Argument length error!!!  wangjichang jiayou");
        }
        if (!objectInspectors[0].getCategory().equals(ObjectInspector.Category.PRIMITIVE)) {
            throw new UDFArgumentTypeException(0,"Input Args Type Error!!!");
        }

        //函数本身返回值为int，需要返回int类型的鉴别器对象，这里鉴别的是最终的结果
        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    /*
    * 用于核心逻辑
    * */
    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        Object o = deferredObjects[0].get();
        if (o == null) {
            return 0;
        }else{
            return o.toString().length();
        }
    }

    /*
    * 不用
    * */
    @Override
    public String getDisplayString(String[] strings) {
        return null;
    }
}
