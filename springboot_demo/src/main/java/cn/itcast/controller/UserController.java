package cn.itcast.controller;

import cn.itcast.bean.User;
import cn.itcast.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * @description:
 * @author: huang
 * @create: 2020-09-09 22:10
 */

@RestController
@RequestMapping("/user")
public class UserController {
     //注入
    @Autowired
    UserService userService;

    @GetMapping("/hello")
    public String hello(String name){
        return "hello:" + name;
    }



    //需求1:接收post请求并保存数据到MySQL
    //url: localhost:8081/user/add
    //post请求的参数可以在方法体中
    //username=jack
    //password=888
    //nickname=jackma
    @RequestMapping("add")//也可以是@PostMapping，Pos请求
    public String addUser(User user){
        //调用service将user保存到数据库
        userService.save(user);
        return "success";
    }

    //需求2:查询所有用户并返回
    @RequestMapping("findAll")//或者getMapping("findAll")
    public List<User> findAll(){
        return userService.findAll();
    }
    //需求:查询所有一个用户并返回
    @RequestMapping("/findById/{id}")
    public User findById(@PathVariable  Long id){
        return userService.findById( id);
    }

    //需求3:根据username和password查询
    //传统的数据请求路径格式:localhost:8888/user/findByUsernameAndPassword?username=jack&password=888
    //现在流行RestFul风格:localhost:8888/user/findByUsernameAndPassword/jack/888
    @RequestMapping("findByUsernameAndPassword/{username}/{password}")
    public List<User> findByUsernameAndPassword(@PathVariable String username, @PathVariable String password){
        return userService.findByUsernameAndPassword(username,password);
    }

    //需求4:根据username和password查询,并执行指定的sql
    @RequestMapping("findBySQL/{username}/{password}")
    public List<User> findBySQL(@PathVariable String username, @PathVariable String password){
        return userService.findBySQL(username,password);
    }

}
