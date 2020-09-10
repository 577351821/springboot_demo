package com.itheima.bean

/**
 * pageview模型存储的表结构数据
 * @param session
 * @param remote_addr
 * @param time_local
 * @param request
 * @param visit_step
 * @param page_staylong
 * @param htp_referer
 * @param http_user_agent
 * @param body_bytes_send
 * @param status
 * @param guid
 */
case class PageViewsBeanCase(session: String,
                             remote_addr: String,
                             time_local: String,
                             request: String,
                             visit_step: Int,
                             page_staylong: Long,
                             htp_referer: String,
                             http_user_agent: String,
                             body_bytes_send: String,
                             status: String,
                             guid: String)
