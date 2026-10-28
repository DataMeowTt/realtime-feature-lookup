package rtfl.gen;

import java.util.LinkedHashSet;
import java.util.Random;
import java.util.Set;

public final class UserSampler {
    /** Seed cố định để Stack A và B chọn đúng cùng một nhóm hot user. */
    private static final long HOT_USERS_SEED = 7;

    private final Random random;
    private final int n;
    private final int[] hotUsers;
    private final double hotShare;
    private final String description;

    private UserSampler(Random random, int n, int[] hotUsers, double hotShare, String description) {
        this.random = random;
        this.n = n;
        this.hotUsers = hotUsers;
        this.hotShare = hotShare;
        this.description = description;
    }

    public static UserSampler uniform(int n, Random random) {
        return new UserSampler(random, n, null, 0, "uniform");
    }

    /**
     * Hot key: hotShare giao dịch dồn vào hotCount user cố định, phần còn lại chọn đều trên toàn bộ user.
     * Mỗi hot user tự rơi vào đúng 1 partition/bucket và 1 node lưu trữ ở cả hai stack.
     */
    public static UserSampler hotUsers(int n, int hotCount, double hotShare, Random random) {
        if (hotCount <= 0 || hotCount >= n) {
            throw new IllegalArgumentException("hot-users must be in 1.." + (n - 1) + ", got " + hotCount);
        }
        Random pick = new Random(HOT_USERS_SEED);
        Set<Integer> hot = new LinkedHashSet<>();
        while (hot.size() < hotCount) {
            hot.add(pick.nextInt(n) + 1);
        }
        int[] arr = hot.stream().mapToInt(Integer::intValue).toArray();
        String desc = String.format("hot (%d hot users / %d, share=%.0f%%)", hotCount, n, hotShare * 100);
        return new UserSampler(random, n, arr, hotShare, desc);
    }

    public int next() {
        if (hotUsers != null && random.nextDouble() < hotShare) {
            return hotUsers[random.nextInt(hotUsers.length)];
        }
        return random.nextInt(n) + 1;
    }

    @Override
    public String toString() {
        return description;
    }
}
