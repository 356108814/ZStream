package com.ztesoft.zstream;

import java.util.List;
import java.util.Map;

/**
 * 默认action
 *
 * @author Yuri
 */
public class DefaultActionExtProcessor implements ActionExtProcessor {
    @Override
    public void process(List<Map<String, Object>> rows) {
        for (Map<String, Object> row: rows) {
            System.out.println(row.toString());
        }
    }
}
