package com.kafka.web;

import com.kafka.config.ApplicationConfiguration;
import com.kafka.kafka.KafkaProducerUtil;
import com.kafka.pojo.User;
import com.kafka.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;


/**
 * 
* Title: UserRestController
* Description: 
* 用户数据操作接口
* Version:1.0.0  
* @author pancm
* @date 2018年1月9日
 */
@RestController
@RequestMapping(value = "/api")
public class UserRestController {
	@Autowired
    private UserService userService;
	@Autowired
	ApplicationConfiguration app;
	
    @GetMapping("/user")
    public List<User> findByUser(User user) {
    	System.out.println("开始查询...");
        return userService.findByUser(user);
    }
    
    @PostMapping("/user")
    public boolean sendKafka(@RequestBody User user) {
        return KafkaProducerUtil.sendMessage(user.toString(), app.getServers(), app.getTopicName());
    }
}
