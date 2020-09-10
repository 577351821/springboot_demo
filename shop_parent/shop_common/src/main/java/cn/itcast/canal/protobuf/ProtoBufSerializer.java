package cn.itcast.canal.protobuf;

import cn.itcast.canal.bean.CanalRowData;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * 根据分析org.apache.kafka.common.serialization.StringSerializer字符串序列化的方式，发现如果需要实现自定义序列化，需要集成implements接口，而该接口需要指定一个泛型（可以是任意类型）
 */
public class ProtoBufSerializer implements Serializer<ProtoBufable> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, ProtoBufable data) {
        return data.toBytes();
    }

    @Override
    public void close() {

    }
}
