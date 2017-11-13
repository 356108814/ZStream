package com.ztesoft.zstream;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 作业配置
 *
 * @author Yuri
 */
public class JobConf {
    private String name;
    private String desc;
    private String engineType;
    private List<Map<String, Object>> processors;
    private Map<String, Object> params;

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

    /**
     * 获取数据源配置
     *
     * @return 数据源列表
     */
    public List<Map<String, Object>> getSourceProcessors() {
        return getSourceProcessorsByType("source");
    }

    /**
     * 获取转换计算配置
     *
     * @return 数据源列表
     */
    public List<Map<String, Object>> getTransformProcessors() {
        return getSourceProcessorsByType("transform");
    }

    /**
     * 获取动作配置
     *
     * @return 数据源列表
     */
    public List<Map<String, Object>> getActionProcessors() {
        return getSourceProcessorsByType("action");
    }

    private List<Map<String, Object>> getSourceProcessorsByType(String type) {
        List<Map<String, Object>> processors = new ArrayList<>();
        for (Map<String, Object> p : this.processors) {
            if (p.get("type").equals(type)) {
                processors.add(p);
            }
        }
        return processors;
    }

    @Override
    public String toString() {
        return "JobConf{" +
                "name='" + name + '\'' +
                ", desc='" + desc + '\'' +
                ", engineType='" + engineType + '\'' +
                ", processors=" + processors +
                ", params=" + params +
                '}';
    }
}
