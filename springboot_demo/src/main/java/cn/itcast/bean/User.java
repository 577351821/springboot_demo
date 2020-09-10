package cn.itcast.bean;



/**
 * @description:
 * @author: huang
 * @create: 2020-09-09 22:16
 */
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import javax.persistence.*;

@Data //表示使用lombok增加各种重复代码，如get，set等
@Entity //标识当前类为SpringDataJPA的实体类，会和数据库中的表相对应
@JsonIgnoreProperties({"hibernateLazyInitializer","handler"})//这一串固定的, 用来处理将数据库的数据转为json并返回
public class User {
    @Id //当前字段对应于数据库中的ID
    @GeneratedValue(strategy = GenerationType.IDENTITY)//按照数据库自己的自增方式进行自增
    private Long id;
//    @Column(name = "username")//表示该字段和数据库中的指定列相对应,如果名称一样可以省略
    private String username;
    private String password;
    private String nickname;//昵称
}