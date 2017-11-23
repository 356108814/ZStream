package com.ztesoft.zstream;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * action扩展处理接口
 *
 * @author Yuri
 */
public interface ActionExtProcessor extends Serializable {
    /**
     * 数据处理
     *
     * @param rows 行列表
     */
    void process(List<Map<String, Object>> rows);
}
