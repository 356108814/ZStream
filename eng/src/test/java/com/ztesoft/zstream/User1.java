package com.ztesoft.zstream;

import com.alibaba.fastjson.JSON;

/**
 * @author Yuri
 * @create 2017-11-10 17:48
 */
public class User1 {
    private Integer id;
    private String name;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public static void main(String[] args) {
        String json="{\"name\":\"chenggang\",\"id\":1}";
        User1 user = JSON.parseObject(json, User1.class);
        System.out.println(user);
    }
}
