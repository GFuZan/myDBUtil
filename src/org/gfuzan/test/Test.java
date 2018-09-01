package org.gfuzan.test;

import java.sql.SQLException;

import org.gfuzan.model.User;
import org.gfuzan.util.DBUtil;

public class Test {

	public static void main(String[] args) {
		testUpdate();
/*		String sql ="select * from tb_user;";
		Object param[]={};
		List<User> list = db.getObjectList(User.class,sql, param);
		for (User user : list) {
			System.out.println(user);
		}*/
		System.out.println("**************");
		String sql ="select * from tb_user where id=?;";
		Object param[]={2};
		User user = DBUtil.getObject(User.class,sql, param);
		System.out.println(user);
	}
	@org.junit.Test
	public static void testUpdate(){
		User user=new User();
		user.setAddress("啦啦啦");
		user.setLoginname("aaa");
		user.setPassword("abcabc");
		user.setPhone("010101");
		String sql="insert into tb_user(loginname,password,phone,address) values(?,?,?,?);";
		Object[] param ={user.getLoginname(),user.getPassword(),user.getPhone(),user.getPhone()};
		System.out.println(DBUtil.update(param,sql));
	}
	@org.junit.Test
	public static  void testUpdateT(){
		User user=new User();
		user.setAddress("啦啦啦");
		user.setLoginname("aaa");
		user.setPassword("abcabc");
		user.setPhone("010101");
		
		String sql="insert into tb_user(loginname,password,phone,address) values(?,?,?,?);";
		Object[] param ={user.getLoginname(),user.getPassword(),user.getPhone(),user.getPhone()};
		DBUtil dbutil=new DBUtil();
		dbutil.openTransaction();
		try {
			dbutil.updateTransaction(sql, param);
			sql+="aaa";
			dbutil.updateTransaction(sql, param);
			dbutil.commitTransaction();
		} catch (SQLException e) {
			dbutil.rollbackTransaction();
		}
		
	}
}
