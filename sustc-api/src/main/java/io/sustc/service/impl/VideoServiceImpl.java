package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.PostVideoReq;
import io.sustc.dto.VideoRecord;
import io.sustc.service.VideoService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Random;
import java.util.Set;

@Service
@Slf4j
public class VideoServiceImpl implements VideoService {
    @Autowired
    private DataSource dataSource;
    private static final String CHARACTERS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    /**
     * Posts a video. Its commit time shall be {@link LocalDateTime#now()}.
     *
     * @param auth the current user's authentication information
     * @param req  the video's information
     * @return the video's {@code bv}
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>{@code auth} is invalid, as stated in {@link io.sustc.service.UserService#deleteAccount(AuthInfo, long)}</li>
     *   <li>{@code req} is invalid
     *     <ul>
     *       <li>{@code title} is null or empty</li>
     *       <li>there is another video with same {@code title} and same user</li>
     *       <li>{@code duration} is less than 10 (so that no chunk can be divided)</li>
     *       <li>{@code publicTime} is earlier than {@link LocalDateTime#now()}</li>
     *     </ul>
     *   </li>
     * </ul>
     * If any of the corner case happened, {@code null} shall be returned.
     */
    @Override
    public String postVideo(AuthInfo auth, PostVideoReq req) {
        // auth is valid
        if (!isValidAuth(auth)) {
            return null;
        }

        // title empty
        if (req.getTitle().isEmpty()) return null;
        //same title
        if(isTitleDuplicate(req.getTitle())) return null;
        //duration
        if(req.getDuration() < 10) return null;
        if(req.getPublicTime().before(Timestamp.valueOf(LocalDateTime.now())))return null;
        String bv = generateBv();

        // 创建VideoRecord对象
        VideoRecord videoRecord = new VideoRecord(auth.getMid());
        videoRecord.setBv(bv); // todo
        videoRecord.setTitle(req.getTitle());
        videoRecord.setOwnerMid(auth.getMid());
//        videoRecord.setOwnerName(UserServiceImpl.getUserInfo(auth.getMid())); // todo
        videoRecord.setCommitTime(Timestamp.valueOf(LocalDateTime.now()));
        videoRecord.setReviewTime(null); // todo
        videoRecord.setPublicTime(req.getPublicTime());
        videoRecord.setDuration(req.getDuration());
        videoRecord.setDescription(req.getDescription());
        videoRecord.setReviewer(null); // todo

        ;

        // TODO: 设置其他字段，如like、coin、favorite等

        // 将VideoRecord对象导入数据库
//        importVideoRecord(videoRecord);

        return bv;
    }
    private boolean isValidAuth(AuthInfo auth) {
        // both qq and Wechat are non-empty while they do not correspond to same user
        if(auth.getQq().isEmpty() && auth.getWechat().isEmpty())return false;
        // mid is invalid while qq and wechat are both invalid (empty or not found)
        // todo
        return true;
    }

    private boolean isValidVideo(PostVideoReq req) {
        // 检查视频标题是否为空或为空字符串
        if (req.getTitle() == null || req.getTitle().isEmpty()) {
            return false;
        }

        // 检查视频时长是否小于10秒
        if (req.getDuration() < 10) {
            return false;
        }

        // 检查视频公开时间是否早于当前时间
        if (req.getPublicTime() != null && req.getPublicTime().before(Timestamp.valueOf(LocalDateTime.now()))) {
            return false;
        }

        // TODO: 检查视频标题和用户的唯一性
        if(false) return false;

        return true;
    }

    private String generateBv() {
        String bv;
        do {
            StringBuilder sb = new StringBuilder(10);
            Random random = new Random();
            for (int i = 0; i < 10; i++) {
                int index = random.nextInt(CHARACTERS.length());
                char randomChar = CHARACTERS.charAt(index);
                sb.append(randomChar);
            }
            bv = "BV" + sb;
        } while (isBVDuplicate(bv));
        return bv;
    }
    // true--duplicate
    private boolean isBVDuplicate(String bv) {
        boolean isDuplicate = false;
        String query = "SELECT bv FROM videos WHERE bv = '?'";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setString(1, bv);
            ResultSet resultSet = stmt.executeQuery();
            if (resultSet.next()) {
                isDuplicate = true;
            }
            resultSet.close();
            stmt.close();
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return isDuplicate;
    }
    private boolean isTitleDuplicate(String title) {
        boolean isDuplicate = false;
        String query = "SELECT title FROM videos WHERE title = '?'";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setString(1, title);
            ResultSet resultSet = stmt.executeQuery();
            if (resultSet.next()) {
                isDuplicate = true;
            }
            resultSet.close();
            stmt.close();
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return isDuplicate;
    }

    private boolean isMidDuplicate(long mid) {
        boolean isDuplicate = false;
        String query = "SELECT mid FROM videos WHERE mid = '?'";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setString(1, String.valueOf(mid));
            ResultSet resultSet = stmt.executeQuery();
            if (resultSet.next()) {
                isDuplicate = true;
            }
            resultSet.close();
            stmt.close();
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return isDuplicate;
    }

    private String getUserName ( long mid){
        // TODO: 根据用户的mid获取用户名
        return "User" + mid;
    }
    /**
     * Deletes a video.
     * This operation can be performed by the video owner or a superuser.
     *
     * @param auth the current user's authentication information
     * @param bv   the video's {@code bv}
     * @return success or not
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>{@code auth} is invalid, as stated in {@link io.sustc.service.UserService#deleteAccount(AuthInfo, long)}</li>
     *   <li>cannot find a video corresponding to the {@code bv}</li>
     *   <li>{@code auth} is not the owner of the video nor a superuser</li>
     * </ul>
     * If any of the corner case happened, {@code false} shall be returned.
     */
    @Override
    public boolean deleteVideo(AuthInfo auth, String bv) {
        // if auth is invalid

        //
        return false;
    }

    @Override
    public boolean updateVideoInfo(AuthInfo auth, String bv, PostVideoReq req) {
        return false;
    }

    @Override
    public List<String> searchVideo(AuthInfo auth, String keywords, int pageSize, int pageNum) {
        return null;
    }

    @Override
    public double getAverageViewRate(String bv) {
        return 0;
    }

    @Override
    public Set<Integer> getHotspot(String bv) {
        return null;
    }

    @Override
    public boolean reviewVideo(AuthInfo auth, String bv) {
        return false;
    }

    @Override
    public boolean coinVideo(AuthInfo auth, String bv) {
        return false;
    }

    @Override
    public boolean likeVideo(AuthInfo auth, String bv) {
        return false;
    }

    @Override
    public boolean collectVideo(AuthInfo auth, String bv) {
        return false;
    }
}
