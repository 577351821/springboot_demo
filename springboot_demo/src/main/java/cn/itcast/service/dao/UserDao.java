package cn.itcast.service.dao;

import cn.itcast.bean.User;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * @description:
 * @author: huang
 * @create: 2020-09-09 22:39
 */
@Repository
public interface UserDao extends JpaRepository<User,Long> {
    //使用SpringDataJPA的好处就是:
    //1.可以直接使用父接口中继承过来的方法-add-findall-getone
    //2.可以根据方法名自动生成sql: select * from user where username = ? and password = ?
    List<User> findByUsernameAndPassword(String username, String password);

    //3.还可以指定自定义的HQL(hibernateSQL,SpringDataJPA底层用的Hibernae)
    //HQL是一种面向对象的SQL
    @Query("select u from User u where u.username = ?1 and u.password = ?2")
    List<User> findBySQL(String username, String password);

}
