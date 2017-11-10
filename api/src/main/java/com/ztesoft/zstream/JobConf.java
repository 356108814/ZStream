package com.ztesoft.zstream;

import java.util.List;
import java.util.Map;

/**
 * 作业配置
 *
 * @author Yuri
 * @create 2017-11-10 17:05
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
