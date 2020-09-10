package cn.itcast.canal.bean;

import cn.itcast.canal.protobuf.CanalModel;
import com.alibaba.fastjson.JSON;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.HashMap;
import java.util.Map;

/**
 * @description:
 * @author: huang
 * @create: 2020-08-27 18:49
 */
public class CanalRowData {
    /**
     * 定义构造方法，解析map对象的binlog日志
     */
    public CanalRowData(Map map){
        this.logfileName = map.get("logfileName").toString();
        this.logfileOffset = Long.parseLong(map.get("logfileOffset").toString());
        this.executeTime = Long.parseLong(map.get("executeTime").toString());
        this.schemaName = map.get("schemaName").toString();
        this.tableName = map.get("tableName").toString();
        this.eventType = map.get("eventType").toString();
        this.columns = (Map<String, String>)map.get("columns");
    }

    /**
     * 传递一个字节码的数组，将字节码数据反序列化成对象返回
     * @param bytes
     */
    public CanalRowData(byte[] bytes){
        try {
            //将字节码数据反序列化成对象
            CanalModel.RowData rowData = CanalModel.RowData.parseFrom(bytes);
            this.logfileName = rowData.getLogfileName();
            this.logfileOffset = rowData.getLogfileOffset();
            this.executeTime = rowData.getExecuteTime();
            this.schemaName = rowData.getSchemaName();
            this.tableName = rowData.getTableName();
            this.eventType = rowData.getEventType();

            //将所有的列的集合添加到map集合中
            this.columns = new HashMap<>();
            this.columns.putAll(rowData.getColumnsMap());
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
    }

    /**
     * 返回binglog日志转换成的protobuf格式数据
     * @return
     */

    public byte[] toBytes(){
        CanalModel.RowData.Builder builder = CanalModel.RowData.newBuilder();
        builder.setLogfileName(this.logfileName);
        builder.setLogfileOffset(this.logfileOffset);
        builder.setExecuteTime(this.executeTime);
        builder.setSchemaName(this.schemaName);
        builder.setTableName(this.tableName);
        builder.setEventType(this.eventType);
        for (String key : this.getColumns().keySet()) {
            builder.putColumns(key, this.getColumns().get(key));
        }

        //将传递的binlog日志解析后序列化成字节码数据返回
        return builder.build().toByteArray();
    }


    private String logfileName;
    private Long logfileOffset;
    private Long executeTime;
    private String schemaName;

    public String getLogfileName() {
        return logfileName;
    }

    public void setLogfileName(String logfileName) {
        this.logfileName = logfileName;
    }

    public Long getLogfileOffset() {
        return logfileOffset;
    }

    public void setLogfileOffset(Long logfileOffset) {
        this.logfileOffset = logfileOffset;
    }

    public Long getExecuteTime() {
        return executeTime;
    }

    public void setExecuteTime(Long executeTime) {
        this.executeTime = executeTime;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public Map<String, String> getColumns() {
        return columns;
    }

    public void setColumns(Map<String, String> columns) {
        this.columns = columns;
    }

    private String tableName;
    private String eventType;
    private Map<String, String> columns;

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
