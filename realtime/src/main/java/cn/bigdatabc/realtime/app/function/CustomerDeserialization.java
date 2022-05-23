package cn.bigdatabc.realtime.app.function;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

/**
 * @description：TODO
 * @author     ：bboy枫亭
 * @date       ：Created in 2022/5/23 20:06
 */
public class CustomerDeserialization implements DebeziumDeserializationSchema {
    /**
     * 封装的数据格式
     * {
     * "database":"",
     * "tableName":"",
     * "before":{"id":"","tm_name":""....},
     * "after":{"id":"","tm_name":""....},
     * "type":"c u d",
     * //"ts":156456135615
     * }
     */
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector collector) throws Exception {
        //1.创建JSON对象用于存储最终数据
        JSONObject result = new JSONObject();

        //2.获取库名&表名
        String topic = sourceRecord.topic();
        String[] fields = topic.split("\\.");
        String dataBase = fields[1];
        String tableName = fields[2];

        Struct value = (Struct) sourceRecord.value();
        //3.获取"before"数据
        Struct beforeStruct = value.getStruct("before");
        JSONObject before = new JSONObject();
        if (beforeStruct != null) {
            Schema beforeSchema = beforeStruct.schema();
            List<Field> beforeFieldList = beforeSchema.fields();
            for (Field field: beforeFieldList) {
                Object beforeValue = beforeStruct.get(field);
                before.put(field.name(), beforeValue);
            }
        }

        //4.获取"after"数据
        Struct afterStruct = value.getStruct("after");
        JSONObject after = new JSONObject();
        if (afterStruct != null) {
            Schema afterSchema = afterStruct.schema();
            List<Field> afterFieldList = afterSchema.fields();
            for (Field field: afterFieldList) {
                after.put(field.name(), afterStruct.get(field));
            }
        }

        //5.获取操作类型  CREATE UPDATE DELETE
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.toString().toLowerCase();
        if ("create".equals(type)) {
            type = "insert";
        }

        //6.将字段写入JSON对象
        result.put("database", dataBase);
        result.put("tableName", tableName);
        result.put("before", before);
        result.put("after", after);
        result.put("type", type);

        //7.输出数据
        collector.collect(result.toJSONString());
    }

    @Override
    public TypeInformation getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }

}
