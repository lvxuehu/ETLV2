/*
*  时间： 2007-7-24 11:30:16
*  北京完美时空网络技术有限公司
*/
package com.wanmei.gamelog.db;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by IntelliJ IDEA.
 * 作者: 李亮阳
 * 日期: 2007-7-24
 * 时间: 11:30:16
 * 版本: 1.0
 * 数据源相关操作方法
 */


public class DBManager {

    //新活动数据库
    private static DataSource huodongdataSource218 = null;//大数据量数据库
    private static DataSource huodongdataSource226 = null;//普通据量数据库
    private static DataSource odi = null;//odi数据库
    private static DataSource coupon = null;//coupon数据库

    static {
        try {
            System.out.println("------datasource init successful------------");
        } catch (Exception e) {
            System.out.println("------datasource init fails------------");
            e.printStackTrace();
        }
    }

    /**
     * 私有的构造函数
     */
    private DBManager() {
    }

    /**
     * 初始化数据源
     * huodong数据库，226
     *
     * @return
     * @throws javax.naming.NamingException
     */
    private static DataSource initDbHuodong226() throws NamingException {
        if (huodongdataSource226 == null) {
            Context initCtx = new InitialContext();
            Context envCtx = (Context) initCtx.lookup("java:comp/env");
            DataSource source = (DataSource) envCtx.lookup("jdbc/huodong226");
            return source;
        } else {
            return huodongdataSource226;
        }
    }

     /**
     * 初始化数据源
     * huodong数据库，218
     *
     * @return
     * @throws javax.naming.NamingException
     */
    private static DataSource initDbHuodong218() throws NamingException {
        if (huodongdataSource218 == null) {
            Context initCtx = new InitialContext();
            Context envCtx = (Context) initCtx.lookup("java:comp/env");
            DataSource source = (DataSource) envCtx.lookup("jdbc/huodong218");
            return source;
        } else {
            return huodongdataSource218;
        }
    }
    
     /**
     * 初始化数据源
     * huodong数据库，odi
     *
     * @return
     * @throws javax.naming.NamingException
     */
    private static DataSource initDbOdi() throws NamingException {
        if (odi == null) {
            Context initCtx = new InitialContext();
            Context envCtx = (Context) initCtx.lookup("java:comp/env");
            DataSource source = (DataSource) envCtx.lookup("jdbc/odi");
            return source;
        } else {
            return odi;
        }
    }
    
    
     /**
     * 初始化数据源
     * huodong数据库，coupon
     *
     * @return
     * @throws javax.naming.NamingException
     */
    private static DataSource initDbCoupon() throws NamingException {
        if (coupon == null) {
            Context initCtx = new InitialContext();
            Context envCtx = (Context) initCtx.lookup("java:comp/env");
            DataSource source = (DataSource) envCtx.lookup("jdbc/coupon");
            return source;
        } else {
            return coupon;
        }
    }
    
      /**
     * 得到默认数据库链接，默认为大流量数据库 218
     *
     * @return connection
     * @throws Exception
     */
    public static Connection getHuodongConnection() throws Exception {
        Connection conn = null;

        // 大流量数据库218
        if (huodongdataSource218 != null) {
            conn = huodongdataSource218.getConnection();
        } else {
            huodongdataSource218 = initDbHuodong218();
            conn = huodongdataSource218.getConnection();
        }

        return conn;
    }
    /**
     * 得到数据库链接，注意：用完后务必关闭连接
     *
     * @return connection
     * @throws Exception
     */
    public static Connection getHuodongConnection(String hddbname) throws Exception {
        Connection conn = null;
        if (hddbname.equals("huodong226")) {
            // 普通流量数据库226
            if (huodongdataSource226 != null) {
                conn = huodongdataSource226.getConnection();
            } else {
                huodongdataSource226 = initDbHuodong226();
                conn = huodongdataSource226.getConnection();
            }
        } else if (hddbname.equals("huodong218")) {
            // 大数据量数据库218
            if (huodongdataSource218 != null) {
                conn = huodongdataSource218.getConnection();
            } else {
                huodongdataSource218 = initDbHuodong218();
                conn = huodongdataSource218.getConnection();
            }
        } else if (hddbname.equals("odi")) {
            // 大数据量数据库218
            if (odi != null) {
                conn = odi.getConnection();
            } else {
                odi = initDbOdi();
                conn = odi.getConnection();
            }
        }else if (hddbname.equals("coupon")) {
            // 大数据量数据库218
            if (coupon != null) {
                conn = coupon.getConnection();
            } else {
                coupon = initDbCoupon();
                conn = coupon.getConnection();
            }
        }
        return conn;
    }

    /**
     * 关闭连接
     *
     * @param connection
     * @throws java.sql.SQLException
     */
    public static void closeConn(Connection connection) throws SQLException {
        if (connection != null) {
            if (!connection.isClosed())
                connection.close();
        }

    }

    /**
     * 关闭  statement
     *
     * @param statement
     * @throws java.sql.SQLException
     */
    public static void closeStat(Statement statement) throws SQLException {
        if (statement != null) {
            statement.close();
        }
    }


    /**
     * 关闭  resultSet
     *
     * @param resultSet
     */
    public static void closeResult(ResultSet resultSet) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * 关闭 result，stat ，conn
     *
     * @param result
     * @param stat
     * @param conn
     */
    public static void close(ResultSet result, Statement stat, Connection conn) {
        try {
            closeResult(result);
            closeStat(stat);
            closeConn(conn);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
