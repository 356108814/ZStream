package com.ztesoft.zstream;

import java.io.File;
import java.util.List;

/**
 * @author Yuri
 */
public class TestSourceExtProcessor implements SourceExtProcessor {
    @Override
    public boolean filterFile(String filePath) {
        System.out.println(filePath);
        File file = new File(filePath);
        System.out.println("文件大小：" + file.length());
        return true;
    }

    @Override
    public boolean filterLine(String line, String format, List<ColumnDef> columnDefs) {
        return true;
    }

    @Override
    public String transformLine(String line, String format, List<ColumnDef> columnDefs) {
        return line;
    }
}
