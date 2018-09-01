package org.gfuzan.util;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DBUtil {
	private Connection conn = null;

/*	private static final String jdbcDriver = "com.mysql.jdbc.Driver";
	private static final String jdbcUrl = "jdbc:mysql://localhost:3306/mybatis_spring";
	private static final String userName = "gfuzan";
	private static final String password = "sql111";

	static {
		try {
			Class.forName(jdbcDriver);
		} catch (ClassNotFoundException e) {
			System.err.println("类: " + jdbcDriver + " 没有找到!!!");
		}
	}*/

	
/*	public static <T> T update(T obj, String sql,Object param[]) {
		Class<?> type=obj.getClass();
		Map<String, Object> map = update(param, sql, true);
		Set<String> keySet = map.keySet();
		Object o=null;
		
		for (String  key: keySet) {
			o=map.get(key);
			Field field = null;
			try {
				field = type.getDeclaredField(key);
			} catch (NoSuchFieldException | SecurityException e) {
				System.err.println("创建实例" + type + "失败");
				return null;
			}
			try {
				field.set(obj, o);
			} catch (IllegalArgumentException | IllegalAccessException e) {
				System.err.println("实体类:" + type + "中找不到属性: " + key);
				continue;
			}
		}
		return obj;
	}*/
	
	/**
	 * 执行insert语句
	 * 
	 * @param param
	 *            sql语句中参数
	 * @param sql
	 *            update语句
	 * @return 新增数据主键
	 */
	public static Object update(Object param[], String sql) {
		Map<String, Object> map = update(param, sql, true);
		Set<String> keySet = map.keySet();
		Object o=null;
		for (String  key: keySet) {
			o=map.get(key);
		}
		return o;
	}

	
	private static Map<String, Object> update(Object param[],String sql,boolean tag) {
		Connection conn = getConnection();
		PreparedStatement ps = null;
		ResultSet rs = null;
		Object o = null;
		String key=null;
		Map<String, Object> m=new HashMap<>();
		try {
			ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			setPreparedStatementParam(ps, param);
			ps.executeUpdate();
			rs = ps.getGeneratedKeys();
			ResultSetMetaData metaData = rs.getMetaData();
			key = metaData.getColumnName(1);
			if (rs.next()) {
				o = rs.getObject(1);
			}
		} catch (SQLException e) {
			System.err.println("执行失败,请检查sql语句==> "+sqlShow(sql, param));
		} finally {
			closeResultSet(rs);
			closeStatement(ps);
			closeConnection(conn);
		}
		m.put(key, o);
		return m;
	}
	/**
	 * 执行insert,delete,update语句
	 * 
	 * @param sql
	 *            insert,delete,update语句
	 * @param param
	 *            sql语句中参数
	 * @return 受影响行数
	 */
	public static int update(String sql, Object param[]) {
		Connection conn = getConnection();
		int r = 0;
		PreparedStatement ps = null;
		try {
			ps = conn.prepareStatement(sql);
			setPreparedStatementParam(ps, param);
			r = ps.executeUpdate();
		} catch (SQLException e) {
			System.err.println("sql执行失败,请检查sql语句==>  "+sqlShow(sql, param));
		} finally {
			closeStatement(ps);
			closeConnection(conn);
		}
		return r;
	}

	/**
	 * 开启事务
	 * 
	 * @return 操作结果
	 */
	public boolean openTransaction() {
		boolean b = false;
		if (conn != null) {
			System.err.println("事务已存在");
			return b;
		}
		conn = getConnection();
		if (conn != null) {
			try {
				conn.setAutoCommit(false);
				b = true;
			} catch (SQLException e) {
				System.err.println("设置手动提交失败");
			}
		}
		return b;
	}

	/**
	 * 提交事务
	 * @return 操作结果
	 */
	public boolean commitTransaction() {
		boolean b = false;
		try {
			conn.commit();
			conn.setAutoCommit(true);
			closeConnection(conn);
			b = true;
		} catch (SQLException e) {
			System.err.println("提交事务失败");
		}
		return b;
	}

	/**
	 * 回滚事务
	 * @return 操作结果
	 */
	public boolean rollbackTransaction() {
		boolean b = false;
		try {
			conn.rollback();
			conn.setAutoCommit(true);
			closeConnection(conn);
			b = true;
		} catch (SQLException e) {
			System.err.println("回滚事务失败");
		}
		return b;
	}

	/**
	 * 执行update语句(事务)
	 * 
	 * @param param
	 *            sql语句中参数
	 * @param sql
	 *            update语句
	 * @return 新增数据主键
	 * @throws SQLException
	 *             执行失败
	 */
	public Object updateTransaction(Object param[], String sql) throws SQLException {
		boolean b = true;
		PreparedStatement ps = null;
		ResultSet rs = null;
		Object o = null;
		try {
			ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			setPreparedStatementParam(ps, param);
			ps.executeUpdate();
			rs = ps.getGeneratedKeys();
			if (rs.next()) {
				o = rs.getObject(1);
			}
		} catch (SQLException e) {
			b = false;
		} finally {
			closeResultSet(rs);
			closeStatement(ps);
			if (!b) {
				String msg = "执行失败,请检查sql语句==> "+sqlShow(sql, param);
				System.err.println(msg);
				throw new SQLException(msg);
			}
		}
		return o;
	}

	/**
	 * 执行insert,delete,update语句(事务)
	 * 
	 * @param sql
	 *            insert,delete,update语句
	 * @param param
	 *            sql语句中参数
	 * @return 受影响行数
	 * @throws SQLException
	 *             执行失败
	 */
	public int updateTransaction(String sql, Object param[]) throws SQLException {
		int r = 0;
		boolean b = true;
		PreparedStatement ps = null;
		try {
			ps = conn.prepareStatement(sql);
			setPreparedStatementParam(ps, param);
			r = ps.executeUpdate();
		} catch (SQLException e) {
			b = false;
		} finally {
			closeStatement(ps);
			if (!b) {
				String msg = "执行失败,请检查sql语句==> "+sqlShow(sql, param);
				System.err.println(msg);
				throw new SQLException(msg);
			}
		}
		return r;
	}

	/**
	 * 查询数据库
	 * 
	 * @param type
	 *            实体类
	 * @param sql
	 *            查询语句
	 * @param param
	 *            sql语句中参数
	 * @return 对象数组
	 */
	public static <T> T getObject(Class<T> type, String sql, Object param[]) {
		List<T> list = getObjectList(type, sql, param);
		if (list != null && list.size() > 0) {
			return list.get(0);
		}
		return null;
	}

	/**
	 * 查询数据库
	 * 
	 * @param type
	 *            实体类
	 * @param sql
	 *            查询语句
	 * @param param
	 *            sql语句中参数
	 * @return 对象数组
	 */
	public static <T> List<T> getObjectList(Class<T> type, String sql, Object param[]) {
		List<Map<String, Object>> lm = query(sql, param);
		List<T> lt = new ArrayList<>();
		for (int i = 0; i < lm.size(); i++) {
			Set<String> keySet = lm.get(i).keySet();
			T o = null;
			try {
				o = type.newInstance();
			} catch (InstantiationException | IllegalAccessException e1) {
				System.err.println("创建实例" + type + "失败");
				return null;
			}
			for (String key : keySet) {
				Field field = null;
				try {
					field = type.getDeclaredField(key);
				} catch (NoSuchFieldException | SecurityException e) {
					System.err.println("实体类:" + type + "中找不到属性: " + key);
					continue;
				}
				field.setAccessible(true);
				try {
					field.set(o, lm.get(i).get(key));
				} catch (IllegalArgumentException | IllegalAccessException e) {
					System.err.println("实体类:" + type + "中属性: " + key + " 类型是否与数据库一致");
				}

			}
			lt.add(o);

		}

		return lt;

	}

	/**
	 * 查询数据库
	 * 
	 * @param sql
	 *            查询语句
	 * @param param
	 *            sql语句中参数
	 * @return 以列名为键值返回表中数据
	 */
	public static List<Map<String, Object>> query(String sql, Object param[]) {
		Connection conn = getConnection();
		PreparedStatement prepareStatement = null;
		ResultSet rs = null;
		List<Map<String, Object>> lm = new ArrayList<>();
		try {
			prepareStatement = conn.prepareStatement(sql);
			setPreparedStatementParam(prepareStatement, param);
		} catch (SQLException e) {
			System.err.println("获取Statement失败");
			return lm;
		}
		try {
			rs = prepareStatement.executeQuery();
			ResultSetMetaData metaData = rs.getMetaData();
			int count = metaData.getColumnCount();
			while (rs.next()) {
				Map<String, Object> m = new HashMap<>();
				for (int i = 0; i < count; i++) {
					m.put(metaData.getColumnName(i + 1), rs.getObject(i + 1));
				}
				lm.add(m);
			}
		} catch (SQLException e) {
			System.err.println("查询失败,请检查sql语句: "+sqlShow(sql, param));
		} finally {
			closeResultSet(rs);
			closeStatement(prepareStatement);
			closeConnection(conn);
		}
		return lm;
	}

	/**
	 * 获取数据库连接
	 * 
	 * @return 数据库连接
	 */
	public static Connection getConnection() {
		Connection connection = null;
		try {
//			connection = DriverManager.getConnection(jdbcUrl, userName, password);
			connection=DBDataSource.getConnectionC3P0();
		} catch (Exception e) {
			System.err.println("获取数据库连接失败");
		}
		return connection;
	}

	/**
	 * 关闭数据库连接
	 * 
	 * @param conn
	 *            要关闭的连接
	 * @return 执行状态
	 */
	public static boolean closeConnection(Connection conn) {
		boolean b = false;
		if (conn != null) {
			try {
				conn.close();
				b = true;
			} catch (SQLException e) {
				System.err.println("关闭Connection失败");
			}
		}
		return b;
	}

	/**
	 * 关闭Statement连接
	 * 
	 * @param stat
	 *            要关闭的Statement
	 * @return 执行状态
	 */
	public static boolean closeStatement(Statement stat) {
		boolean b = false;
		if (stat != null) {
			try {
				stat.close();
				b = true;
			} catch (SQLException e) {
				System.err.println("关闭Statement失败");
			}
		}
		return b;
	}

	/**
	 * 关闭ResultSet连接
	 * 
	 * @param rs
	 *            要关闭的ResultSet
	 * @return 执行状态
	 */
	public static boolean closeResultSet(ResultSet rs) {
		boolean b = false;
		if (rs != null) {
			try {
				rs.close();
				b = true;
			} catch (SQLException e) {
				System.err.println("关闭ResultSet失败");
			}
		}
		return b;
	}

	private static void setPreparedStatementParam(PreparedStatement ps, Object param[]) {
		if (ps == null || param == null) {
			return;
		}
		for (int i = 0; i < param.length; i++) {
			try {
				ps.setObject(i + 1, param[i]);
			} catch (SQLException e) {
				System.err.println("sql参数赋值失败,请检查sql中占位符个数与类型是否与参数数组一致");
			}
		}
	}
	private static String sqlShow(String sql,Object[] param){
		if(param==null||param.length==0){
			return sql;
		}
		StringBuilder sb=new StringBuilder();
		String[] split = sql.split("\\?");
		sb.append(split[0]).append("'").append(param[0]).append("'");
		for (int i = 1; i < split.length-1; i++) {
			sb.append(",'").append(param[i]).append("'");
		}
		sb.append(split[split.length-1]);
		return sb.toString();
		
	}
}
