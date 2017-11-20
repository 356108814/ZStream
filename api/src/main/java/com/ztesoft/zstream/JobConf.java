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
    private String engineType;
    private List<Map<String, Object>> processors;
    private Map<String, Object> params;
    private Map<String, String> tableDef;
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
