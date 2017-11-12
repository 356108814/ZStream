package com.ztesoft.zstream;

import java.util.List;

/**
 * 数据源扩展处理器
 *
 * @author Yuri
 */
public interface SourceExtProcessor {
    /**
     * 过滤
     *
     * @param line       原始行数据
     * @param format     值为json或其他分隔符如：,
     * @param columnDefs 字段定义列表
     * @return true表示需要处理
     */
    boolean filter(String line, String format, List<ColumnDef> columnDefs);

    /**
     * 过滤
     *
     * @param line       原始行数据
     * @param format     值为json或其他分隔符如：,
     * @param columnDefs 字段定义列表
     *
     * @return 转换后的数据
     */
    String transform(String line, String format, List<ColumnDef> columnDefs);
}
