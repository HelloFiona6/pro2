package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.RegisterUserReq;
import io.sustc.dto.UserInfoResp;
import io.sustc.service.UserService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.util.Random;

@Service
@Slf4j
public class UserServiceImpl implements UserService {
    @Autowired
    private DataSource dataSource;

    @Override
    public long register(RegisterUserReq req) {
        if (req.getPassword() == null || req.getName() == null || req.getSex() == null) {
            return -1L;
        }
        int a = req.getBirthday().indexOf("ÔÂ");
        int b = req.getBirthday().indexOf("ÈÕ");
        if (a == -1 || b == -1 || a >= b) return -1;
        String month = req.getBirthday().substring(0, a);
        String day = req.getBirthday().substring(a + 1, b);
        int Month = Integer.parseInt(month);
        int Day = Integer.parseInt(day);
        if (!(Month >= 1 && Month <= 12)) return -1;
        int[][] arr = {
                {1, 31},
                {2, 29},
                {3, 31},
                {4, 30},
                {5, 31},
                {6, 30},
                {7, 31},
                {8, 31},
                {9, 30},
                {10, 31},
                {11, 30},
                {12, 31},
        };

        if (Day > arr[a - 1][1]) {
            return -1;
        }

        String sqlOfQQ = " select count(*) as count from table where QQ= " + req.getQq() + ";";
        int numberOfQQ = 0;
        try (Connection conn = dataSource.getConnection();
             Statement statement = conn.createStatement();
             ResultSet resultSet = statement.executeQuery(sqlOfQQ)) {

            while (resultSet.next()) {
                numberOfQQ = resultSet.getInt("count");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        if (numberOfQQ != 0) return -1;

        String sqlOfWechat = " select count(*) as count from table where QQ= " + req.getWechat() + ";";
        int numberOfWechat = 0;
        try (Connection conn = dataSource.getConnection();
             Statement statement = conn.createStatement();
             ResultSet resultSet = statement.executeQuery(sqlOfWechat)) {

            while (resultSet.next()) {
                numberOfWechat = resultSet.getInt("count");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        if (numberOfWechat != 0) return -1;

        Random r = new Random();
        return r.nextLong(Long.MAX_VALUE) + 1;
//        return -1;
    }

    @Override
    public boolean deleteAccount(AuthInfo auth, long mid) {
        return false;
    }

    @Override
    public boolean follow(AuthInfo auth, long followeeMid) {
        return false;
    }

    @Override
    public UserInfoResp getUserInfo(long mid) {
        String sql = "select count(*) over(partition by name) as count,name as name from UserRecord where mid= " + mid + ";";
        int number = 0;
        String name = null;
        try (Connection conn = dataSource.getConnection();
             Statement statement = conn.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {

            while (resultSet.next()) {
                number = resultSet.getInt("count");
                name = resultSet.getString("name");
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        if (number == 0) return null;
        else return new UserInfoResp();
    }
}
