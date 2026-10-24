package rtfl.gen;

import java.util.Random;

public final class UserSampler {
    private final Random random;
    private final int n;
    private final int[] hotUsers;
    private final int[] coldUsers;
    private final double hotShare;
    private final String description;

    private UserSampler(Random random, int n, int[] hotUsers, int[] coldUsers, double hotShare, String description) {
        this.random = random;
        this.n = n;
        this.hotUsers = hotUsers;
        this.coldUsers = coldUsers;
        this.hotShare = hotShare;
        this.description = description;
    }

    public static UserSampler uniform(int n, Random random) {
        return new UserSampler(random, n, null, null, 0, "uniform");
    }

    public static UserSampler hot(int[] partitionOf, int hotPartition, double hotShare, Random random, String hotBy) {
        int n = partitionOf.length - 1;
        int hotCount = 0;
        for (int k = 1; k <= n; k++) {
            if (partitionOf[k] == hotPartition) hotCount++;
        }
        if (hotCount == 0 || hotCount == n) {
            throw new IllegalArgumentException("partition " + hotPartition + " has " + hotCount + "/" + n + " users");
        }
        int[] hot = new int[hotCount];
        int[] cold = new int[n - hotCount];
        int h = 0, c = 0;
        for (int k = 1; k <= n; k++) {
            if (partitionOf[k] == hotPartition) hot[h++] = k;
            else cold[c++] = k;
        }
        String desc = String.format("hot (hot-by=%s partition=%d share=%.0f%%, %d hot users / %d)",
                hotBy, hotPartition, hotShare * 100, hotCount, n);
        return new UserSampler(random, n, hot, cold, hotShare, desc);
    }

    public int next() {
        if (hotUsers == null) {
            return random.nextInt(n) + 1;
        }
        int[] group = random.nextDouble() < hotShare ? hotUsers : coldUsers;
        return group[random.nextInt(group.length)];
    }

    @Override
    public String toString() {
        return description;
    }
}
