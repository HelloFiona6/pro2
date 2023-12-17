package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.service.RecommenderService;

import java.util.List;

public class RecommenderServiceImpl implements RecommenderService {
    @Override
    public List<String> recommendNextVideo(String bv) {
        return null;
    }

    @Override
    public List<String> generalRecommendations(int pageSize, int pageNum) {
        return null;
    }

    @Override
    public List<String> recommendVideosForUser(AuthInfo auth, int pageSize, int pageNum) {
        return null;
    }

    @Override
    public List<Long> recommendFriends(AuthInfo auth, int pageSize, int pageNum) {
        return null;
    }
}
