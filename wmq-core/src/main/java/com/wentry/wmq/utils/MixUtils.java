package com.wentry.wmq.utils;

import com.google.common.collect.Lists;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

/**
 * @Description:
 * @Author: tangwc
 */
public class MixUtils {
    public static <T> T randomRm(Collection<T> src) {
        if (src.isEmpty()) {
            return null;
        }

        // 将 Set 转换为 List
        List<T> list = new ArrayList<>(src);

        // 生成随机索引
        Random rand = new Random();
        int index = rand.nextInt(list.size());

        T res = list.get(index);
        // 从 源集合中移除元素
        src.remove(res);
        return res;
    }

    public static Integer randomGetIdx(List<?> src, List<Integer> excludes) {
        if (CollectionUtils.isEmpty(src)) {
            return null;
        }
        excludes = ListUtils.emptyIfNull(excludes);
        List<Integer> selectIdxes = new ArrayList<>();
        for (int i = 0; i < src.size(); i++) {
            if (!excludes.contains(i)) {
                selectIdxes.add(i);
            }
        }
        if (CollectionUtils.isEmpty(selectIdxes)) {
            return null;
        }
        return randomGet(selectIdxes);
    }

    public static <T> T randomGet(Collection<T> src) {
        if (src.isEmpty()) {
            return null;
        }

        // 将 Set 转换为 List
        List<T> list = new ArrayList<>(src);

        // 生成随机索引
        Random rand = new Random();
        int index = rand.nextInt(list.size());

        //随机获取
        return list.get(index);
    }

    public static void main(String[] args) {
        for (int i = 0; i < 199; i++) {
            System.out.println(randomGet(Lists.newArrayList(0, 1)));
        }
    }

    public static String consumerInstanceKey(String topic, Integer partition, String consumerGroup) {
        return String.join(":", topic, String.valueOf(partition), consumerGroup);
    }

}
