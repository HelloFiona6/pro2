package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.service.DanmuService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@Service
@Slf4j
public class DanmuServiceImpl implements DanmuService {
    @Autowired
    private DataSource dataSource;

    @Override
    public long sendDanmu(AuthInfo auth, String bv, String content, float time) {
        if (!validAuth(auth)) return -1;

        //bv存在
        if (!existBv(bv)) return -1;

        //内容合法
        if (content == null || content.isEmpty()) return -1;

        //视频公布
        String sqlOfPublic = "select public_time::timestamp as public_time from video where owner_mid = ?";
        String current = "select current_timestamp";
        Timestamp publicTime = null;
        Timestamp currentTime = null;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfPublic)) {
            stmt.setLong(1, auth.getMid());

            ResultSet resultSet = stmt.executeQuery(sqlOfPublic);
            if (resultSet.next()) {
                publicTime = resultSet.getTimestamp("public_time");
            }
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(current)) {
            ResultSet resultSet = stmt.executeQuery(current);
            if (resultSet.next()) {
                currentTime = resultSet.getTimestamp("public_time");
            }
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        assert publicTime != null;
        if (publicTime.after(currentTime)) {
            return -1;
        }


        //是否观看
        String sqlOfWatch = "select count(*) as count from view where video_bv = ? and user_mid = ?";
        int numberOfWatch = 0;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfWatch)) {
            stmt.setString(1, bv);
            stmt.setLong(2, auth.getMid());

            ResultSet resultSet = stmt.executeQuery(sqlOfWatch);
            if (resultSet.next()) {
                numberOfWatch = resultSet.getInt("count");
            }
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        if (numberOfWatch != 1) return -1;


        //最大弹幕id
        String sqlOfMaxMid = " select max(danmu_id) as max from danmu";
        long MaxID = 0L;
        try (Connection conn = dataSource.getConnection();
             Statement statement = conn.createStatement();
             ResultSet resultSet = statement.executeQuery(sqlOfMaxMid)) {
            while (resultSet.next()) {
                MaxID = resultSet.getInt("max");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return MaxID + 1;
    }

    @Override
    public List<Long> displayDanmu(String bv, float timeStart, float timeEnd, boolean filter) {
        if (!existBv(bv)) return null;

        //获取时长
        String sqlOfDuration = "select duration from video where bv = ? ";
        float duration = 0;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfDuration)) {
            stmt.setString(1, bv);

            ResultSet resultSet = stmt.executeQuery(sqlOfDuration);
            if (resultSet.next()) {
                duration = resultSet.getFloat("duration");
            }
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        //合法时间
        if (timeEnd <= timeStart || timeEnd > duration || timeEnd < 0 || timeStart < 0 || timeStart > duration) {
            return null;
        }

        //视频公布
        String sqlOfPublic = "select public_time::timestamp as public_time from video where bv = ?";
        String current = "select current_timestamp";
        Timestamp publicTime = null;
        Timestamp currentTime = null;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfPublic)) {
            stmt.setString(1, bv);

            ResultSet resultSet = stmt.executeQuery(sqlOfPublic);
            if (resultSet.next()) {
                publicTime = resultSet.getTimestamp("public_time");
            }
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(current)) {
            ResultSet resultSet = stmt.executeQuery(current);
            if (resultSet.next()) {
                currentTime = resultSet.getTimestamp("public_time");
            }
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        assert publicTime != null;
        if (publicTime.after(currentTime)) {
            return null;
        }

        //获取List
        List<Long> list = new ArrayList<>();

        String sqlOfWatch = "select ? danmu_id as id from danmu where time between ? and ? order by post_time";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfWatch)) {
            if (filter) stmt.setString(1, "distinct");
            else stmt.setString(1, "");
            stmt.setFloat(2, timeStart);
            stmt.setFloat(3, timeEnd);

            ResultSet resultSet = stmt.executeQuery(sqlOfWatch);
            while (resultSet.next()) {
                list.add(resultSet.getLong("id"));
            }
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return list;
    }

    @Override
    public boolean likeDanmu(AuthInfo auth, long id) {
        if (!validAuth(auth)) return false;
        else return existDanmu(id);
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

        if (!existMid(auth.getMid()) && (auth.getQq() == null || !existQQ(auth.getQq())) && (auth.getWechat() == null || !existWechat(auth.getWechat()))) {
            return true;
        }
        return false;
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

    public boolean existBv(String bv) {
        String sqlOfBv = "select count(*) as count from video where bv = ?";
        int numberOfBv = 0;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfBv)) {
            stmt.setString(1, bv);

            ResultSet resultSet = stmt.executeQuery(sqlOfBv);
            if (resultSet.next()) {
                numberOfBv = resultSet.getInt("count");
            }
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return numberOfBv == 1;
    }

    public boolean existDanmu(long id) {
        String sqlOfBv = "select count(*) as count from danmu where id = ?";
        int numberOfBv = 0;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfBv)) {
            stmt.setLong(1, id);

            ResultSet resultSet = stmt.executeQuery(sqlOfBv);
            if (resultSet.next()) {
                numberOfBv = resultSet.getInt("count");
            }
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return numberOfBv == 1;
    }
}
