package com.example.kafka.demo.entity;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

@Table(name = "customer")
public class Customer {
    @Id
    @Column(name = "id")
    @GeneratedValue
    private Integer id;
    private String name;
    private Integer sex;
    private String mobike;
    private String location;

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

    public Integer getSex() {
        return sex;
    }

    public void setSex(Integer sex) {
        this.sex = sex;
    }

    public String getMobike() {
        return mobike;
    }

    public void setMobike(String mobike) {
        this.mobike = mobike;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }
}

