package com.ztesoft.zstream;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 作业配置
 *
 * @author Yuri
 */
public class JobConf implements Serializable {
    /**
     * 作业唯一标识
     */
    private String id;
    private String name;
    private String desc;
    /**
     * 引擎类型，spark或flink
     */
    private String engineType;
    /**
     * 处理器配置，类型包括source、dim、transform、action
     */
    private List<Map<String, Object>> processors;
    /**
     * 参数，包含用户自定义参数、spark参数、系统参数等
     * 用${name}作占位符
     * spark参数以spark开头，如spark.master
     */
    private Map<String, Object> params;
    /**
     * 输出表定义
     */
    private Map<String, String> tableDef;
    /**
     * 自定义函数配置
     */
    private Map<String, String> udf;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public String getEngineType() {
        return engineType;
    }

    public void setEngineType(String engineType) {
        this.engineType = engineType;
    }

    public List<Map<String, Object>> getProcessors() {
        return processors;
    }

    public void setProcessors(List<Map<String, Object>> processors) {
        this.processors = processors;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    public void setParams(Map<String, Object> params) {
        this.params = params;
    }

    public Map<String, String> getTableDef() {
        return tableDef;
    }

    public void setTableDef(Map<String, String> tableDef) {
        this.tableDef = tableDef;
    }

    /**
     * 赋值参数，替换参数占位符为实际值
     */
    public void assignParams() {
        int i = 0;
        for (Map<String, Object> p : this.processors) {
            for (Map.Entry<String, Object> entry : p.entrySet()) {
                String name = entry.getKey();
                String value = entry.getValue().toString();
                if (value.startsWith("${")) {
                    String key = value.replace("${", "").replace("}", "");
                    if (this.params.containsKey(key)) {
                        value = this.params.get(key).toString();
                        this.processors.get(i).put(name, value);
                    }
                }
            }
            i++;
        }
    }

    /**
     * 获取数据源配置
     *
     * @return 数据源配置列表
     */
    public List<Map<String, Object>> getSourceProcessors() {
        return getProcessorsByType("source");
    }

    /**
     * 获取维度表配置
     *
     * @return 维度表列表
     */
    public List<Map<String, Object>> getDimProcessors() {
        return getProcessorsByType("dim");
    }

    /**
     * 获取转换计算配置
     *
     * @return 转换配置列表
     */
    public List<Map<String, Object>> getTransformProcessors() {
        return getProcessorsByType("transform");
    }

    /**
     * 获取动作配置
     *
     * @return action配置列表
     */
    public List<Map<String, Object>> getActionProcessors() {
        return getProcessorsByType("action");
    }

    private List<Map<String, Object>> getProcessorsByType(String type) {
        List<Map<String, Object>> processors = new ArrayList<>();
        for (Map<String, Object> p : this.processors) {
            if (p.get("type").equals(type)) {
                processors.add(p);
            }
        }
        return processors;
    }

    public Map<String, String> getUdf() {
        return udf;
    }

    public void setUdf(Map<String, String> udf) {
        this.udf = udf;
    }

    /**
     * 当启用了累加计算时，必须设置checkpoint
     *
     * @return true，需要设置
     */
    public boolean isNeedSetCheckpoint() {
        boolean isNeed = false;
        List<Map<String, Object>> transformProcessors = getTransformProcessors();
        for (Map<String, Object> map : transformProcessors) {
            if (map.containsKey("acc") && Boolean.parseBoolean(map.get("acc").toString())) {
                isNeed = true;
            }
        }
        return isNeed;
    }

    /***
     * 获取输出表名称列表
     *
     * @return 名称列表
     */
    public List<String> getOutputTableNames() {
        List<String> names = new ArrayList<>();
        for (Map<String, Object> p : this.processors) {
            String type = p.get("type").toString();
            if ("source".equals(type) || "transform".equals(type)) {
                names.add(p.get("outputTableName").toString());
            }
        }
        return names;
    }

    @Override
    public String toString() {
        return "JobConf{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", desc='" + desc + '\'' +
                ", engineType='" + engineType + '\'' +
                ", processors=" + processors +
                ", params=" + params +
                ", tableDef=" + tableDef +
                ", udf=" + udf +
                '}';
    }
}
