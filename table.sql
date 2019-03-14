CREATE TABLE `customer` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) DEFAULT NULL COMMENT '客户名',
  `sex` int(2) DEFAULT NULL COMMENT '客户性别 1 男，2女 ，3未知',
  `mobile` varchar(15) DEFAULT NULL COMMENT '客户地址',
  `location` varchar(100) DEFAULT NULL COMMENT '家庭住址',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8;


CREATE TABLE `order` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `goods` varchar(255) DEFAULT NULL COMMENT '商品名称',
  `order_date` datetime DEFAULT NULL COMMENT '订单日期',
  `cust_id` int(11) DEFAULT NULL COMMENT '客户id',
  `note` varchar(100) DEFAULT NULL COMMENT '订单备注',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8