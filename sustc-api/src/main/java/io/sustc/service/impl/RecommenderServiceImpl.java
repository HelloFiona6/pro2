package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.service.RecommenderService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Service
@Slf4j
public class RecommenderServiceImpl implements RecommenderService {
    @Autowired
    private DataSource dataSource;

    {
        String sqlFunctionRecommend_Videos_For_User = """
                CREATE OR REPLACE FUNCTION Recommend_Videos_For_User(current_mid BIGINT)
                    RETURNS TEXT[]
                    LANGUAGE plpgsql
                AS $$
                DECLARE
                    all_mid         BIGINT[];
                    friend_mid      BIGINT[];
                    my_bv           TEXT[];
                    friend_bv       TEXT[];
                    oneMid          BIGINT;
                    follower        INT;
                    following       INT;
                    text_mid        TEXT;
                    text_friend_mid TEXT;
                    text_my_bv      TEXT;
                    recommend       TEXT[];
                BEGIN
                    all_mid := ARRAY(SELECT DISTINCT mid FROM users WHERE mid <> current_mid);
                    text_mid := array_to_string(all_mid, ',');
                                
                    FOREACH oneMid IN ARRAY all_mid LOOP
                        SELECT COUNT(*) INTO follower FROM follower WHERE follower = current_mid AND following = oneMid;
                        SELECT COUNT(*) INTO following FROM follower WHERE follower = oneMid AND following = current_mid;
                        IF follower = 1 AND following = 1 THEN
                            friend_mid := array_append(friend_mid, oneMid);
                        END IF;
                    END LOOP;
                                
                    text_friend_mid := array_to_string(friend_mid, ',');
                    my_bv := ARRAY(SELECT DISTINCT video_BV FROM View WHERE user_mid = current_mid);
                    text_my_bv := array_to_string(my_bv, ',');
                    friend_bv := ARRAY(SELECT DISTINCT video_BV FROM View WHERE View.video_BV NOT IN (SELECT UNNEST(my_bv)));
                                
                    recommend := ARRAY(SELECT bv
                                       FROM (SELECT owner_mid, bv, count, public_time
                                             FROM (SELECT video_BV, COUNT(*) OVER (PARTITION BY video_BV) AS count
                                                   FROM view
                                                   WHERE video_BV IN (SELECT UNNEST(friend_bv))
                                                   GROUP BY video_BV
                                                   ORDER BY count DESC) a
                                                   LEFT JOIN video ON a.video_BV = Video.BV) b
                                                LEFT JOIN users ON b.owner_mid = Users.mid
                                       ORDER BY level DESC, count DESC, public_time DESC);
                    RETURN recommend;
                END;
                $$;
                """;


        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlFunctionRecommend_Videos_For_User)) {
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    {
        String Recommend_Friends = """
                   create or replace function Recommend_Friends(current_mid BIGINT)
                       RETURNS BIGINT[]
                       language plpgsql
                   AS
                   $$
                   DECLARE
                       my_following          BIGINT[];
                       text_my_following     TEXT;
                       common_following      BIGINT[];
                       text_common_following TEXT;
                   begin
                       my_following := ARRAY(select following_mid from follow where follower_mid = current_mid);
                       text_my_following := array_to_string(my_following, ',');
                       common_following := ARRAY(select follower_mid
                                                 from (select distinct follower_mid, count(*) as count
                                                       from follow
                                                       where follower_mid <> current_mid
                                                         and following_mid in (select unnest(my_following))
                                                       group by follower_mid) a
                                                          left join users on Users.mid = a.follower_mid
                                                 order by count desc, level desc);
                       return common_following;
                   end;
                   $$;
                """;


        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(Recommend_Friends)) {
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public List<String> recommendNextVideo(String bv) {
        String sqlOfBv = "select count(*) as count from video where bv=?";
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
        if (numberOfBv == 0) return null;

        //BvViewer
        String sqlOfBvViewer = "select user_mid from video where bv=?";
        List<String> listOfViewer = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfBvViewer)) {
            stmt.setString(1, bv);
            ResultSet resultSet = stmt.executeQuery(sqlOfBvViewer);
            while (resultSet.next()) {
                listOfViewer.add(resultSet.getString("user_mid"));
            }
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        StringBuilder viewer = new StringBuilder();
        for (int i = 0; i < listOfViewer.size(); i++) {
            if (i != listOfViewer.size() - 1) viewer.append(listOfViewer.get(i)).append(",");
            else viewer.append(listOfViewer.get(i));
        }
        viewer = new StringBuilder("(" + viewer + ")");


        //BvList
        String sqlOfBvList = "select user_mid from video where bv <> ?";
        List<String> listOfBv = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfBvList)) {
            stmt.setString(1, bv);
            ResultSet resultSet = stmt.executeQuery(sqlOfBvList);
            while (resultSet.next()) {
                listOfBv.add(resultSet.getString("user_mid"));
            }
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }


        List<node> temp = new ArrayList<>();
        String sqlOfViewWithNumber = "select count(*) as count from view where video_BV = ? and user_mid in ?";
        for (int i = 0; i < listOfBv.size(); i++) {
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sqlOfViewWithNumber)) {
                stmt.setString(1, listOfBv.get(i));
                stmt.setString(2, String.valueOf(viewer));
                ResultSet resultSet = stmt.executeQuery(sqlOfBvList);
                while (resultSet.next()) {
                    temp.add(new node(listOfBv.get(i), resultSet.getInt("count")));
                }
                stmt.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        Collections.sort(temp);
        List<String> result = new ArrayList<>();
        for (int i = 0; i < Math.min(5, temp.size()); i++) {
            result.add(temp.get(i).bv);
        }
        return result;
    }

    @Override
    public List<String> generalRecommendations(int pageSize, int pageNum) {
        if (pageNum <= 0 || pageSize <= 0) return null;
        //BvList
        String sqlOfBvList = "select user_mid from video";
        List<String> listOfBv = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfBvList)) {
            ResultSet resultSet = stmt.executeQuery(sqlOfBvList);
            while (resultSet.next()) {
                listOfBv.add(resultSet.getString("user_mid"));
            }
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        List<Node> temp = new ArrayList<>();
        for (int i = 0; i < listOfBv.size(); i++) {
            double sum = 0;
            //like
            String sqlOfLike = """
                    select count(*) as count from thumbs_up where video_bv = ?
                    """;
            int countOfLike = 0;
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sqlOfLike)) {
                stmt.setString(1, listOfBv.get(i));
                ResultSet resultSet = stmt.executeQuery();
                while (resultSet.next()) {
                    countOfLike = resultSet.getInt("count");
                }
                stmt.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

            //coin
            String sqlOfCoin = """
                    select count(*) as count from coin where video_bv = ?
                    """;
            int countOfCoin = 0;
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sqlOfCoin)) {
                stmt.setString(1, listOfBv.get(i));
                ResultSet resultSet = stmt.executeQuery();
                while (resultSet.next()) {
                    countOfCoin = resultSet.getInt("count");
                }
                stmt.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }


            //fav
            String sqlOfFav = """
                    select count(*) as count from favorite where video_bv = ?
                    """;
            int countOfFav = 0;
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sqlOfFav)) {
                stmt.setString(1, listOfBv.get(i));
                ResultSet resultSet = stmt.executeQuery();
                while (resultSet.next()) {
                    countOfFav = resultSet.getInt("count");
                }
                stmt.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

            //danmu
            String sqlOfDanmu = """
                    select count(*) as count from favorite where bv = ?
                    """;
            int countOfDanmu = 0;
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sqlOfDanmu)) {
                stmt.setString(1, listOfBv.get(i));
                ResultSet resultSet = stmt.executeQuery();
                while (resultSet.next()) {
                    countOfDanmu = resultSet.getInt("count");
                }
                stmt.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

            //finish
            String sqlOfFinish = """
                    select count(*) count from view left join video on view.video_BV = video.BV where last_watch_time_duration=duration and bv=?
                    """;
            int countOfFinish = 0;
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sqlOfFinish)) {
                stmt.setString(1, listOfBv.get(i));
                ResultSet resultSet = stmt.executeQuery();
                while (resultSet.next()) {
                    countOfFinish = resultSet.getInt("count");
                }
                stmt.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

            //total
            String sqlOfView = """
                    select count(*) as count from view where video_bv = ?
                    """;
            int countOfView = 0;
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sqlOfView)) {
                stmt.setString(1, listOfBv.get(i));
                ResultSet resultSet = stmt.executeQuery();
                while (resultSet.next()) {
                    countOfView = resultSet.getInt("count");
                }
                stmt.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            sum += countOfLike * 1.0 / countOfView;
            sum += countOfCoin * 1.0 / countOfView;
            sum += countOfFav * 1.0 / countOfView;
            sum += countOfDanmu * 1.0 / countOfView;
            sum += countOfFinish * 1.0 / countOfView;
            temp.add(new Node(listOfBv.get(i), sum));
        }

        Collections.sort(temp);
        List<String> result = new ArrayList<>();
        for (int i = 0; i < Math.min(temp.size(), pageNum * pageSize); i++) {
            result.add(temp.get(i).bv);
        }
        return result;
    }

    @Override
    public List<String> recommendVideosForUser(AuthInfo auth, int pageSize, int pageNum) {
        if (!validAuth(auth)) return null;
        if (pageSize <= 0 || pageNum <= 0) return null;
        ArrayList<String> arrayList = new ArrayList<>();
        String sql = "select * from recommend_videos_for_user( ? )";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, auth.getMid());
            ResultSet resultSet = stmt.executeQuery();
            while (resultSet.next()) {
                Array array = resultSet.getArray(1);
                String[] values = (String[]) array.getArray();
                arrayList = new ArrayList<>(Arrays.asList(values));
            }
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return arrayList;
    }

    @Override
    public List<Long> recommendFriends(AuthInfo auth, int pageSize, int pageNum) {
        if (!validAuth(auth)) return null;
        if (pageSize <= 0 || pageNum <= 0) return null;
        ArrayList<Long> arrayList = new ArrayList<>();
        String sql = "select * from Recommend_Friends( ? )";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, auth.getMid());
            ResultSet resultSet = stmt.executeQuery();
            while (resultSet.next()) {
                Array array = resultSet.getArray(1);
                Long[] values = (Long[]) array.getArray();
                arrayList = new ArrayList<>(Arrays.asList(values));
            }
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return arrayList;
    }

    private static class node implements Comparable<node> {
        String bv;
        int val;
        double sum;

        public node(String bv, int val) {
            this.bv = bv;
            this.val = val;
        }

        @Override
        public int compareTo(node other) {
            return other.val - this.val;
        }
    }

    private static class Node implements Comparable<Node> {
        String bv;
        double sum;

        public Node(String bv, double sum) {
            this.bv = bv;
            this.sum = sum;
        }

        @Override
        public int compareTo(Node other) {
            return Double.compare(other.sum, this.sum);
        }
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
}