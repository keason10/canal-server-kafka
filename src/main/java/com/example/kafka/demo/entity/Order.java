package com.example.kafka.demo.entity;

import javax.persistence.*;
import java.util.Date;

@Table(name = "order")
public class Order {
    @Id
    @Column(name = "id")
    @GeneratedValue
    private String id;
    private String goods;
    private Date orderDate;
    private Integer custId;
    private String note;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getGoods() {
        return goods;
    }

    public void setGoods(String goods) {
        this.goods = goods;
    }

    public Date getOrderDate() {
        return orderDate;
    }

    public void setOrderDate(Date orderDate) {
        this.orderDate = orderDate;
    }

    public Integer getCustId() {
        return custId;
    }

    public void setCustId(Integer custId) {
        this.custId = custId;
    }

    public String getNote() {
        return note;
    }

    public void setNote(String note) {
        this.note = note;
    }
}
