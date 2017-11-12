package com.ztesoft.zstream;

/**
 * 列定义
 *
 * @author Yuri
 */
public class ColumnDef {
    /**
     * 列名称
     */
    private String name;
    /**
     * 列类型，支持byte、short、int、long、float、double、boolean、date、timestamp
     */
    private String type;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

}
