package com.ztesoft.zstream;

import java.util.List;

/**
 * 数据源扩展处理器
 *
 * @author Yuri
 */
public interface SourceExtProcessor {

    /**
     * 过滤文件
     *
     * @param filePath 文件路径
     * @return true表示需要处理
     */
    boolean filterFile(String filePath);

    /**
     * 过滤行
     *
     * @param line       原始行数据
     * @param format     值为json或其他分隔符如：,
     * @param columnDefs 字段定义列表
     * @return true表示需要处理
     */
    boolean filterLine(String line, String format, List<ColumnDef> columnDefs);

    /**
     * 转换行
     *
     * @param line       原始行数据
     * @param format     值为json或其他分隔符如：,
     * @param columnDefs 字段定义列表
     * @return 转换后的数据
     */
    String transformLine(String line, String format, List<ColumnDef> columnDefs);
}
