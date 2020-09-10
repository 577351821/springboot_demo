package cn.itcast.service.impl;

import cn.itcast.bean.User;
import cn.itcast.service.UserService;
import cn.itcast.service.dao.UserDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @description:
 * @author: huang
 * @create: 2020-09-09 22:38
 */
@Service
public class UserServiceImpl implements UserService {
    @Autowired
    private UserDao userDao;
    @Override
    public void save(User user) {
        //调用dao保存数据到MySQL
        userDao.save(user);
    }

    @Override
    public List<User> findAll() {
        return userDao.findAll();
    }
    @Override
    public User findById(Long id) {
        return userDao.getOne(id);
    }

    @Override
    public List<User> findByUsernameAndPassword(String username, String password) {
        return userDao.findByUsernameAndPassword(username,password);
    }

    @Override
    public List<User> findBySQL(String username, String password) {
        return userDao.findBySQL(username,password);
    }
}
