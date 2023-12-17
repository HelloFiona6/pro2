package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.PostVideoReq;
import io.sustc.dto.VideoRecord;
import io.sustc.service.VideoService;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;

public class VideoServiceImpl implements VideoService {
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
        //
        LocalDateTime.now();
        return null;
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
