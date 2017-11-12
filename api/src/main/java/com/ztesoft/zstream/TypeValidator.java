package com.ztesoft.zstream;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * java数据类型验证器
 *
 * @author Yuri
 */
public class TypeValidator {

    public static boolean isByte(String s) {
        try {
            Byte b = Byte.parseByte(s);
        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }

    public static boolean isShort(String s) {
        try {
            Short st = Short.parseShort(s);
        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }

    public static boolean isInt(String s) {
        try {
            int i = Integer.parseInt(s);
        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }

    public static boolean isLong(String s) {
        try {
            long l = Long.parseLong(s);
        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }

    public static boolean isFloat(String s) {
        try {
            float f = Float.parseFloat(s);
        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }

    public static boolean isDouble(String s) {
        try {
            double d = Double.parseDouble(s);
        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }

    public static boolean isBoolean(String s) {
        return s.equalsIgnoreCase("true") || s.equalsIgnoreCase("false");
    }

    public static boolean isDate(String s) {
        try {
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(s);
            return true;
        } catch (ParseException e) {
            return false;
        }
    }

    public static boolean isTimestamp(String s) {
        try {
            new Timestamp(Long.parseLong(s));
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public static boolean isValid(String s, String type) {
        boolean isValid = false;
        String lcType = type.toLowerCase();
        switch (lcType) {
            case "byte":
                isValid = isByte(s);
                break;
            case "short":
                isValid = isShort(s);
                break;
            case "int":
                isValid = isInt(s);
                break;
            case "long":
                isValid = isLong(s);
                break;
            case "float":
                isValid = isFloat(s);
                break;
            case "double":
                isValid = isDouble(s);
                break;
            case "boolean":
                isValid = isBoolean(s);
                break;
            case "date":
                isValid = isDate(s);
                break;
            case "timestamp":
                isValid = isTimestamp(s);
                break;

            default:
                isValid = true;
                break;

        }
        return isValid;
    }
}
