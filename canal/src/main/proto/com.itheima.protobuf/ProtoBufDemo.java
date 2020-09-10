package com.itheima.protobuf;

import com.google.protobuf.InvalidProtocolBufferException;

public class ProtoBufDemo {
    public static void main(String[] args) throws InvalidProtocolBufferException {
        DemoModel.User.Builder builder = DemoModel.User.newBuilder();
        builder.setId(1);
        builder.setName("张三");
        builder.setSex("男");


        System.out.println(builder.getName());
        System.out.println(builder.getId());
        System.out.println(builder.getSex());

        byte[] bytes = builder.build().toByteArray();
        System.out.println("--protobuf---");
        for (byte b : bytes) {
            System.out.print(b);
        }
        System.out.println();
        System.out.println("---");

        DemoModel.User user = DemoModel.User.parseFrom(bytes);

        System.out.println(user.getName());
    }
}