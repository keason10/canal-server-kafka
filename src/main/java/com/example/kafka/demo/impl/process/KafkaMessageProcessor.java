package com.example.kafka.demo.impl.process;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.example.kafka.demo.dao.CustomerDao;
import com.example.kafka.demo.dao.OrderDao;
import com.example.kafka.demo.elastic.ElasticClient;
import com.example.kafka.demo.entity.Customer;
import net.oschina.durcframework.easymybatis.query.Query;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Component
public class KafkaMessageProcessor {
    @Autowired
    private OrderDao orderDao;
    @Autowired
    private CustomerDao customerDao;

    public void process(List<FlatMessage> messages) throws IOException {
        if (messages == null || messages.isEmpty()) {
            return;
        }
//                        order left join customer
        System.out.println("flat message");
        for (FlatMessage message : messages) {
            //FlatMessage [id=5, database=test, table=canal, isDdl=false, type=UPDATE, es=1552271573000, ts=1552271573926, sql=, sqlType={sex=4, name=12, id=4}, mysqlType={sex=int(3), name=varchar(255), id=int(11)}, data=[{sex=111122, name=12222ee221122, id=11}], old=[{name=12222ee2211}]]
            if (CollectionUtils.isEmpty(message.getData())) {
                continue;
            }
            for (Map<String, String> map : message.getData()) {
                //主表信息处理
                if ("INSERT".equals(message.getType()) && "test.order".equals(message.getDatabase() + "." + message.getTable())) {
                    if (map.get("cust_id") != null) {
                        this.setCustomerInfo(map);
                    }
                    ElasticClient.insert(message.getDatabase(), message.getTable(), map.get("id"), JSON.toJSONString(map));
                }else if ("UPDATE".equals(message.getType()) && "test.order".equals(message.getDatabase() + "." + message.getTable())) {
                    if (map.get("cust_id") != null) {
                        this.setCustomerInfo(map);
                    }
                    ElasticClient.updateWithJson(message.getDatabase(), message.getTable(), map.get("id"), JSON.toJSONString(map));
                }else if ("DELETE".equals(message.getType()) && "test.order".equals(message.getDatabase() + "." + message.getTable())) {
                    ElasticClient.delete(message.getDatabase(), message.getTable(), map.get("id"));
                }

                //副表信息处理
                else if ("INSERT".equals(message.getType()) && "test.customer".equals(message.getDatabase() + "." + message.getTable())) {
                    //not need to process
                } else if ("UPDATE".equals(message.getType()) && "test.customer".equals(message.getDatabase() + "." + message.getTable())) {
                    SearchHits hits = ElasticClient.searchWhere(message.getDatabase(),"order", "cust_id",map.get("id"));
                    if (hits == null) {
                        return;
                    }
                    for (SearchHit hit : hits) {
                        Map<String, Object> mapRet = hit.getSourceAsMap();
                        mapRet.put("mobile",map.get("mobile"));
                        mapRet.put("location", map.get("location"));
                        mapRet.put("sex", map.get("sex"));
                        ElasticClient.updateWithJson("test", "order",mapRet.get("id").toString(), JSON.toJSONString(mapRet));
                    }

                } else if ("DELETE".equals(message.getType()) && "test.customer".equals(message.getDatabase() + "." + message.getTable())){
                    SearchHits hits = ElasticClient.searchWhere(message.getDatabase(), "order", "cust_id", map.get("id"));
                    if (hits == null) {
                        return;
                    }
                    for (SearchHit hit : hits) {
                        Map<String, Object> mapRet = hit.getSourceAsMap();
                        mapRet.put("mobile","");
                        mapRet.put("location","");
                        mapRet.put("sex","");
                        mapRet.put("cust_id","");
                        ElasticClient.updateWithJson("test", "order", mapRet.get("id").toString(), JSON.toJSONString(mapRet));
                    }
                }


            }

        }
    }

    private void setCustomerInfo(Map<String, String> map) {
        List<Customer> customers = customerDao.find(new Query().eq("id", map.get("cust_id")).setPage(0, 1));
        if (customers == null || customers.size() == 0) {
            return;
        }
        map.put("mobile", customers.get(0).getMobike());
        map.put("sex", customers.get(0).getSex() == 1 ? "男" : "女");
        map.put("location", customers.get(0).getLocation());
    }
}
