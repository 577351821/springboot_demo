package cn.itcast.bean

/**
 * visit模型的数据表结构
 * @param guid 				用户id
 * @param session			会话id
 * @param remote_addr	远程访问地址
 * @param inTime			会话进入时间
 * @param outTime			退出会话时间
 * @param inPage			进入页面
 * @param outPage			跳出页面
 * @param referal			跳转页面
 * @param pageVisits	访问页面数量
 */
case class VisitBeanCase(
												guid      :String,
											session			: String,
										 remote_addr	: String,
										 inTime				: String,
										 outTime			: String,
										 inPage			  : String,
										 outPage			: String,
										 referal			: String,
										 pageVisits		: Int )


