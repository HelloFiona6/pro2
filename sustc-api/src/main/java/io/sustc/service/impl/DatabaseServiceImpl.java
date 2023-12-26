package io.sustc.service.impl;

import io.sustc.dto.DanmuRecord;
import io.sustc.dto.UserRecord;
import io.sustc.dto.VideoRecord;
import io.sustc.service.DatabaseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * It's important to mark your implementation class with {@link Service} annotation.
 * As long as the class is annotated and implements the corresponding interface, you can place it under any package.
 */
@Service
@Slf4j
public class DatabaseServiceImpl implements DatabaseService {

    /**
     * Getting a {@link DataSource} instance from the framework, whose connections are managed by HikariCP.
     * <p>
     * Marking a field with {@link Autowired} annotation enables our framework to automatically
     * provide you a well-configured instance of {@link DataSource}.
     * Learn more: <a href="https://www.baeldung.com/spring-dependency-injection">Dependency Injection</a>
     */
    @Autowired
    private DataSource dataSource;

    @Override
    public List<Integer> getGroupMembers() {
        //replace this with your own student IDs in your group
        return Arrays.asList(12210360,12210723);
    }

    @Override
    public void importData(
            List<DanmuRecord> danmuRecords,
            List<UserRecord> userRecords,
            List<VideoRecord> videoRecords
    ) {
        /*
          mid,name,sex,birthday,level,coin,sign,identity,password,qq,wechat
          BV,title,owner_mid,owner_name,commit_time,review_time,public_time,duration,description,reviewer_mid
          video: bv, title, owner_mid, owner_name, commit_time, review_time, public_time, duration, description, reviewer_mid
          follow: follower_mid,following_mid
          thumbs_up: video_BV,user_mid
          coin: video_BV,user_mid
          Favorite: video_BV,user_mid
          View:video_BV, user_mid, last_watch_time_duration
          DanmuLikeBy: danmu_id,mid
         */
        String danmuSql = "INSERT INTO danmu (bv, user_mid, time, content, post_time) VALUES (?, ?, ?, ?, ?)";
        String userSql = "INSERT INTO users (mid, name, sex, birthday, level, coin, sign, identity, password, qq, wechat) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        String videoSql = "INSERT INTO video (bv, title, owner_mid, owner_name, commit_time, review_time, public_time, duration, description, reviewer_mid) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        String followSql = "INSERT INTO follow (follower_mid,following_mid) VALUES (?, ?)";
        String likeSql = "INSERT INTO thumbs_up (video_BV,user_mid) VALUES (?, ?)";
        String coinSql = "INSERT INTO coin (video_BV,user_mid) VALUES (?, ?)";
        String favoriteSql = "INSERT INTO favorite (video_BV,user_mid) VALUES (?, ?)";
        String viewSql = "INSERT INTO view (video_BV, user_mid, last_watch_time_duration) VALUES (?, ?, ?)";
        String DanmuLikeBySql = "INSERT INTO DanmuLikeBy (danmu_id,mid) VALUES (?, ?)";

        long cntDanmu=0;
        long cntUser=0;
        long cntVideo=0;
        long cntFollow=0;
        long cntLike=0;
        long cntCoin=0;
        long cntFavorite=0;
        long cntView=0;
        long cntLikeDanmu=0;
        // todo
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            // 导入用户记录
            try (PreparedStatement userStmt = conn.prepareStatement(userSql);
            PreparedStatement followStmt = conn.prepareStatement(followSql)) {
                for (UserRecord userRecord : userRecords) {
                    userStmt.setLong(1, userRecord.getMid());
                    userStmt.setString(2, userRecord.getName());
                    userStmt.setString(3, userRecord.getSex());
                    userStmt.setString(4, userRecord.getBirthday());
                    userStmt.setShort(5, userRecord.getLevel());
                    userStmt.setInt(6, userRecord.getCoin());
                    userStmt.setString(7, userRecord.getSign());
                    userStmt.setString(8, userRecord.getIdentity().name());
                    userStmt.setString(9, userRecord.getPassword());
                    userStmt.setString(10, userRecord.getQq());
                    userStmt.setString(11, userRecord.getWechat());
                    userStmt.addBatch(); // 添加到批处理
                    cntUser++;
                    for(long follower:userRecord.getFollowing()){
                        followStmt.setLong(1,userRecord.getMid());
                        followStmt.setLong(2,follower);
                        followStmt.addBatch();
                        cntFollow++;

                        if(cntFollow % 100 == 0){
                            followStmt.executeBatch();
                        }
                    }
                    if(cntUser % 100 == 0){
                        userStmt.executeBatch();
                    }
                }
                userStmt.executeBatch();
                followStmt.executeBatch();
            }
            try (PreparedStatement followStmt = conn.prepareStatement(followSql)) {
                for (UserRecord userRecord : userRecords) {
                    for(long follower:userRecord.getFollowing()){
                        followStmt.setLong(1,userRecord.getMid());
                        followStmt.setLong(2,follower);
                        followStmt.addBatch();
                        cntFollow++;

                        if(cntFollow % 100 == 0){
                            followStmt.executeBatch();
                        }
                    }
                }
                followStmt.executeBatch();
            }

            try (PreparedStatement videoStmt = conn.prepareStatement(videoSql);
                 PreparedStatement likeStmt = conn.prepareStatement(likeSql);
                 PreparedStatement coinStmt = conn.prepareStatement(coinSql);
                 PreparedStatement favoriteStmt = conn.prepareStatement(favoriteSql);
                 PreparedStatement viewStmt = conn.prepareStatement(viewSql)
                 ) {
                for (VideoRecord videoRecord : videoRecords) {
                    videoStmt.setString(1, videoRecord.getBv());
                    videoStmt.setString(2, videoRecord.getTitle());
                    videoStmt.setLong(3, videoRecord.getOwnerMid());
                    videoStmt.setString(4, videoRecord.getOwnerName());
                    videoStmt.setTimestamp(5, videoRecord.getCommitTime());
                    videoStmt.setTimestamp(6, videoRecord.getReviewTime());
                    videoStmt.setTimestamp(7, videoRecord.getPublicTime());
                    videoStmt.setFloat(8, videoRecord.getDuration());
                    videoStmt.setString(9, videoRecord.getDescription());
                    videoStmt.setLong(10, videoRecord.getReviewer());
                    videoStmt.addBatch(); // 添加到批处理
                    cntVideo++;
                    for(long like:videoRecord.getLike()){
                        likeStmt.setString(1,videoRecord.getBv());
                        likeStmt.setLong(2,like);
                        likeStmt.addBatch();
                        cntLike++;
                        if(cntLike % 100 == 0){
                            likeStmt.executeBatch();
                        }
                    }
                    for(long coin:videoRecord.getCoin()){
                        coinStmt.setString(1,videoRecord.getBv());
                        coinStmt.setLong(2,coin);
                        coinStmt.addBatch();
                        cntCoin++;
                        if(cntCoin % 100 == 0){
                            coinStmt.executeBatch();
                        }
                    }
                    for(long favorite:videoRecord.getFavorite()){
                        favoriteStmt.setString(1,videoRecord.getBv());
                        favoriteStmt.setLong(2,favorite);
                        favoriteStmt.addBatch();
                        cntFavorite++;
                        if(cntFavorite % 100 == 0){
                            favoriteStmt.executeBatch();
                        }
                    }
                    for (int i = 0; i < videoRecord.getViewerMids().length; i++) {
                        viewStmt.setString(1,videoRecord.getBv());
                        viewStmt.setLong(2,videoRecord.getViewerMids()[i]);
                        viewStmt.setFloat(3,videoRecord.getViewTime()[i]);
                        viewStmt.addBatch();
                        cntView++;
                        if(cntView % 100 == 0){
                            viewStmt.executeBatch();
                        }
                    }
                    if(cntVideo % 100 == 0){
                        videoStmt.executeBatch();
                    }
                }
                videoStmt.executeBatch();
                likeStmt.executeBatch();
                coinStmt.executeBatch();
                favoriteStmt.executeBatch();
                viewStmt.executeBatch();
            }
            // like coin favorite view
            try (PreparedStatement likeStmt = conn.prepareStatement(likeSql);
                 PreparedStatement coinStmt = conn.prepareStatement(coinSql);
                 PreparedStatement favoriteStmt = conn.prepareStatement(favoriteSql);
                 PreparedStatement viewStmt = conn.prepareStatement(viewSql)
            ) {
                for (VideoRecord videoRecord : videoRecords) {
                    for(long like:videoRecord.getLike()){
                        likeStmt.setString(1,videoRecord.getBv());
                        likeStmt.setLong(2,like);
                        likeStmt.addBatch();
                        cntLike++;
                        if(cntLike % 100 == 0){
                            likeStmt.executeBatch();
                        }
                    }
                    for(long coin:videoRecord.getCoin()){
                        coinStmt.setString(1,videoRecord.getBv());
                        coinStmt.setLong(2,coin);
                        coinStmt.addBatch();
                        cntCoin++;
                        if(cntCoin % 100 == 0){
                            coinStmt.executeBatch();
                        }
                    }
                    for(long favorite:videoRecord.getFavorite()){
                        favoriteStmt.setString(1,videoRecord.getBv());
                        favoriteStmt.setLong(2,favorite);
                        favoriteStmt.addBatch();
                        cntFavorite++;
                        if(cntFavorite % 100 == 0){
                            favoriteStmt.executeBatch();
                        }
                    }
                    for (int i = 0; i < videoRecord.getViewerMids().length; i++) {
                        viewStmt.setString(1,videoRecord.getBv());
                        viewStmt.setLong(2,videoRecord.getViewerMids()[i]);
                        viewStmt.setFloat(3,videoRecord.getViewTime()[i]);
                        viewStmt.addBatch();
                        cntView++;
                        if(cntView % 100 == 0){
                            viewStmt.executeBatch();
                        }
                    }
                }
                likeStmt.executeBatch();
                coinStmt.executeBatch();
                favoriteStmt.executeBatch();
                viewStmt.executeBatch();
            }
            String danmuId;
            // 导入弹幕记录
            try (PreparedStatement danmuStmt = conn.prepareStatement(danmuSql);
                 PreparedStatement DanmuLikeByStmt = conn.prepareStatement(DanmuLikeBySql)) {
                for (DanmuRecord danmuRecord : danmuRecords) {
                    danmuStmt.setString(1, danmuRecord.getBv());
                    danmuStmt.setLong(2, danmuRecord.getMid());
                    danmuStmt.setFloat(3, danmuRecord.getTime());
                    danmuStmt.setString(4, danmuRecord.getContent());
                    danmuStmt.setTimestamp(5, danmuRecord.getPostTime());
                    danmuStmt.addBatch(); // 添加到批处理
                    cntDanmu++;
                    danmuId = findDanmuId(danmuRecord.getBv(),danmuRecord.getMid());
                    for(long likeBy:danmuRecord.getLikedBy()){
                        DanmuLikeByStmt.setString(1,danmuId);
                        DanmuLikeByStmt.setLong(2,likeBy);
                        DanmuLikeByStmt.addBatch();
                        cntLikeDanmu++;
                        if(cntLikeDanmu % 100 == 0){
                            DanmuLikeByStmt.executeBatch();
                        }
                    }
                    if(cntDanmu % 100 == 0){
                        danmuStmt.executeBatch();
                    }
                }
                danmuStmt.executeBatch();
                DanmuLikeByStmt.executeBatch();
            }
            conn.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        // print length of every table
        System.out.println(userRecords.size());
        System.out.println(videoRecords.size());
        System.out.println(danmuRecords.size());
    }

    /**
     * The following code is just a quick example of using jdbc datasource.
     * Practically, the code interacts with database is usually written in a DAO layer.
     * <p>
     * Reference: [Data Access Object pattern](https://www.baeldung.com/java-dao-pattern)
     */

    public String findDanmuId(String bv,long mid){
        String sql = "select danmu_id from danmu where bv = ? and user_mid = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, bv);
            stmt.setLong(2, mid);
            log.info("SQL: {}", stmt);

            ResultSet rs = stmt.executeQuery();
            rs.next();
            return rs.getString(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    @Override
    public void truncate() {
        // You can use the default truncate script provided by us in most cases,
        // but if it doesn't work properly, you may need to modify it.

        String sql;

        sql = "DO $$\n" +
                "DECLARE\n" +
                "    tables CURSOR FOR\n" +
                "        SELECT tablename\n" +
                "        FROM pg_tables\n" +
                "        WHERE schemaname = 'public';\n" +
                "BEGIN\n" +
                "    FOR t IN tables\n" +
                "    LOOP\n" +
                "        EXECUTE 'TRUNCATE TABLE ' || QUOTE_IDENT(t.tablename) || ' CASCADE;';\n" +
                "    END LOOP;\n" +
                "END $$;\n";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Integer sum(int a, int b) {
        String sql = "SELECT ?+?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, a);
            stmt.setInt(2, b);
            log.info("SQL: {}", stmt);

            ResultSet rs = stmt.executeQuery();
            rs.next();
            return rs.getInt(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
