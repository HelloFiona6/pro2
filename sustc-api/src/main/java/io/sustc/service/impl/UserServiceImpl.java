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
import java.util.ArrayList;
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
        int a = req.getBirthday().indexOf("月");
        int b = req.getBirthday().indexOf("日");
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

        if (req.getQq() != null) {
            String sqlOfQQ = "select count(*) as count from users where QQ= " + req.getQq() + ";";
            int numberOfQQ = 0;
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement statement = conn.prepareStatement(sqlOfQQ);
                 ResultSet resultSet = statement.executeQuery(sqlOfQQ)) {

                while (resultSet.next()) {
                    numberOfQQ = resultSet.getInt("count");
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            if (numberOfQQ != 0) return -1;
        }

        if (req.getWechat() != null) {
            String sqlOfWechat = " select count(*) as count from users where wechet= " + req.getWechat() + ";";
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
        }

        String sqlOfMaxMid = " select max(mid) as max from users";
        long MaxMid = 0L;
        try (Connection conn = dataSource.getConnection();
             Statement statement = conn.createStatement();
             ResultSet resultSet = statement.executeQuery(sqlOfMaxMid)) {
            while (resultSet.next()) {
                MaxMid = resultSet.getInt("max");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return MaxMid+1;
    }


    @Override
    public boolean deleteAccount(AuthInfo auth, long mid) {
        if (existMid(mid)) return false;

        // auth is invalid
        if (validAuth(auth)) return false;

        String sqlOfIdentity = "select identity as identity from users where mid = ? ";
        String identityOfAuth = "";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfIdentity)) {
            stmt.setLong(1, auth.getMid());
            ResultSet resultSet = stmt.executeQuery(sqlOfIdentity);
            if (resultSet.next()) {
                identityOfAuth = resultSet.getString("identity");
            }
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        String identity = "";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfIdentity)) {
            stmt.setLong(1, mid);
            ResultSet resultSet = stmt.executeQuery(sqlOfIdentity);
            if (resultSet.next()) {
                identity = resultSet.getString("identity");
            }
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }


        if (identityOfAuth.equals("user") && auth.getMid() != mid) {
            return false;
        }
        if (identityOfAuth.equals("superuser") && (!identity.equals("user") || mid != auth.getMid())) {
            return false;
        }
        return true;
    }

    @Override
    public boolean follow(AuthInfo auth, long followeeMid) {

        // auth is invalid
        if (validAuth(auth)) return false;

        String sqlOfNumberOfMid = "select count(*) as count from follow where follower_mid = ? and followee_mid = ?";
        int numberOfBoth = 0;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfNumberOfMid)) {
            stmt.setLong(1, auth.getMid());
            stmt.setLong(2, followeeMid);
            ResultSet resultSet = stmt.executeQuery();
            if (resultSet.next()) {
                numberOfBoth = resultSet.getInt("count");
            }
            resultSet.close();
            stmt.close();
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        if (numberOfBoth != 1) return false;

        return true;
    }

    @Override
    public UserInfoResp getUserInfo(long mid) {
        //mid合法
        String sqlOfNumberOfMid = "select count(*) as count from UserRecord where mid= ?";
        int numberOfMid = 0;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfNumberOfMid)) {
            stmt.setLong(1, mid);
            ResultSet resultSet = stmt.executeQuery();
            if (resultSet.next()) {
                numberOfMid = resultSet.getInt("count");
            }
            resultSet.close();
            stmt.close();
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        if (numberOfMid != 1) return null;

        //coin个数
        String sqlOfNumberOfCoin = """
                select count(owner_mid) from (
                select *
                from coin
                         left join video on coin.video_bv = video.bv)a;""";

        int numberOfCoin = 0;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfNumberOfCoin)) {
            ResultSet resultSet = stmt.executeQuery();
            if (resultSet.next()) {
                numberOfCoin = resultSet.getInt("count");
            }
            resultSet.close();
            stmt.close();
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        //following数组
        String sqlOfFollowing = """
                select following_mid count from follow where follower_mid='?'
                """;
        ArrayList<Long> arrayListOfFollowing = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfFollowing)) {
            stmt.setLong(1, mid);
            ResultSet resultSet = stmt.executeQuery();
            while (resultSet.next()) {
                arrayListOfFollowing.add(resultSet.getLong("count"));
            }
            resultSet.close();
            stmt.close();
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        long[] arrayOfFollowing = new long[arrayListOfFollowing.size()];
        for (int i = 0; i < arrayListOfFollowing.size(); i++) {
            arrayOfFollowing[i] = arrayListOfFollowing.get(i);
        }
        //follower数组
        String sqlOfFollower = """
                select follower_mid as count from follow where following_mid='?'
                """;
        ArrayList<Long> arrayListOfFollower = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfFollower)) {
            stmt.setLong(1, mid);
            ResultSet resultSet = stmt.executeQuery();
            while (resultSet.next()) {
                arrayListOfFollower.add(resultSet.getLong("count"));
            }
            resultSet.close();
            stmt.close();
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        long[] arrayOfFollower = new long[arrayListOfFollower.size()];
        for (int i = 0; i < arrayOfFollower.length; i++) {
            arrayOfFollowing[i] = arrayListOfFollower.get(i);
        }

        //watch数组
        String sqlOfWatch = """
                select video_bv as bv from view where user_mid = ?
                """;
        ArrayList<String> arrayListOfWatch = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfWatch)) {
            stmt.setLong(1, mid);
            ResultSet resultSet = stmt.executeQuery();
            while (resultSet.next()) {
                arrayListOfWatch.add(resultSet.getString("bv"));
            }
            resultSet.close();
            stmt.close();
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        String[] arrayOfWatch = arrayListOfWatch.toArray(new String[0]);


        //like数组
        String sqlOfLike = """
                select video_bv as bv from thumbs_up where user_mid = ?
                """;
        ArrayList<String> arrayListOfLike = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfLike)) {
            stmt.setLong(1, mid);
            ResultSet resultSet = stmt.executeQuery();
            while (resultSet.next()) {
                arrayListOfLike.add(resultSet.getString("bv"));
            }
            resultSet.close();
            stmt.close();
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        String[] arrayOfLike = arrayListOfLike.toArray(new String[0]);

        //collect数组
        String sqlOfCollect = """
                select video_bv as bv from favorite where user_mid='?'
                """;
        ArrayList<String> arrayListOfCollect = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfCollect)) {
            stmt.setLong(1, mid);
            ResultSet resultSet = stmt.executeQuery();
            while (resultSet.next()) {
                arrayListOfCollect.add(resultSet.getString("bv"));
            }
            resultSet.close();
            stmt.close();
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        String[] arrayOfCollect = arrayListOfCollect.toArray(new String[0]);

        //post数组
        String sqlOfPost = """
                select video_bv as bv from video where user_mid='?'            
                """;
        ArrayList<String> arrayListOfPost = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfPost)) {
            stmt.setLong(1, mid);
            ResultSet resultSet = stmt.executeQuery();
            while (resultSet.next()) {
                arrayListOfPost.add(resultSet.getString("bv"));
            }
            resultSet.close();
            stmt.close();
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        String[] arrayOfPost = arrayListOfPost.toArray(new String[0]);


        return new UserInfoResp(mid, numberOfCoin, arrayOfFollowing, arrayOfFollower, arrayOfWatch, arrayOfLike, arrayOfCollect, arrayOfPost);
    }


    public boolean existMid(long mid) {
        String sqlOfMid = "select count(*) as count from users where mid= ?";
        int numberOfMid = 0;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfMid)) {
            stmt.setLong(1, mid);
            ResultSet resultSet = stmt.executeQuery(sqlOfMid);
            if (resultSet.next()) {
                numberOfMid = resultSet.getInt("count");
            }
            resultSet.close();
            stmt.close();
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return numberOfMid != 1;
    }

    public boolean existQQ(String QQ) {
        String sqlOfMid = "select count(*) as count from users where mid= ?";
        int numberOfMid = 0;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfMid)) {
            stmt.setString(1, QQ);
            ResultSet resultSet = stmt.executeQuery(sqlOfMid);
            if (resultSet.next()) {
                numberOfMid = resultSet.getInt("count");
            }
            resultSet.close();
            stmt.close();
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return numberOfMid == 1;
    }

    public boolean existWechat(String Wechat) {
        String sqlOfMid = "select count(*) as count from users where mid= ?";
        int numberOfMid = 0;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfMid)) {
            stmt.setString(1, Wechat);
            ResultSet resultSet = stmt.executeQuery(sqlOfMid);
            if (resultSet.next()) {
                numberOfMid = resultSet.getInt("count");
            }
            resultSet.close();
            stmt.close();
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return numberOfMid == 1;
    }

    public boolean validAuth(AuthInfo auth) {
        // auth is invalid
        String sqlOfWechatAndQQ = "select count(*) as count from users where Wechat= ? or QQ=?";
        int numberOfMid = 0;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfWechatAndQQ)) {
            stmt.setString(1, auth.getWechat());
            stmt.setString(2, auth.getQq());
            ResultSet resultSet = stmt.executeQuery(sqlOfWechatAndQQ);
            if (resultSet.next()) {
                numberOfMid = resultSet.getInt("count");
            }
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        if (auth.getWechat() != null && auth.getQq() != null) {
            return numberOfMid != 1;
        }

        if (existMid(auth.getMid()) && (auth.getQq() == null || !existQQ(auth.getQq())) && (auth.getWechat() == null || !existWechat(auth.getWechat()))) {
            return true;
        }
        return false;
    }

}