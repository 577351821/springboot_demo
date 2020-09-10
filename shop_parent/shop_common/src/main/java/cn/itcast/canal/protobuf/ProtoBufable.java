package cn.itcast.canal.protobuf;

/**
 * 定义一个接口，这个接口中定义toBytes方法
 */
public interface ProtoBufable {
    /**
     * 将对象转换成字节码数组
     * @return
     */
    byte[] toBytes();
}
