package com.ztesoft.zstream;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 默认数据源处理器。先调用filter再调用transform
 *
 * @author Yuri
 */
public class DefaultSourceExtProcessor implements SourceExtProcessor {


    /**
     * 过滤文件
     *
     * @param filePath 文件路径
     * @return true表示需要处理
     */
    @Override
    public boolean filterFile(String filePath) {
        return true;
    }

    /**
     * 数据验证通不过直接丢弃不处理
     *
     * @param line       原始行数据
     * @param format     值为json或其他分隔符如：,
     * @param columnDefs 字段定义列表
     * @return true表示需要处理
     */
    @Override
    public boolean filterLine(String line, String format, List<ColumnDef> columnDefs) {
        if (isJson(format)) {
            JSONObject jsonObject = JSON.parseObject(line);
            if (jsonObject.size() != columnDefs.size()) {
                return false;
            }

            //类型map
            Map<String, String> typeMap = new HashMap<>();
            for (ColumnDef columnDef : columnDefs) {
                typeMap.put(columnDef.getName(), columnDef.getType());
            }

            for (Map.Entry<String, Object> entry : jsonObject.entrySet()) {
                if (!TypeValidator.isValid(entry.getValue().toString(), typeMap.get(entry.getKey()))) {
                    return false;
                }
            }
            return true;

        } else {
            String[] array = line.split(format, -1);
            if (array.length != columnDefs.size()) {
                return false;
            }
            ColumnDef columnDef;
            for (int i = 0; i < array.length; i++) {
                columnDef = columnDefs.get(i);
                if (!TypeValidator.isValid(array[i], columnDef.getType())) {
                    return false;
                }
            }
            return true;
        }
    }

    @Override
    public String transformLine(String line, String format, List<ColumnDef> columnDefs) {
        return line;
    }

    private boolean isJson(String s) {
        return "json".equalsIgnoreCase(s);
    }
}
