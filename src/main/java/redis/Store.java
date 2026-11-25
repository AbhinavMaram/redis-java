package redis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * The keyspace. Not thread-safe by itself: like real Redis, every command runs
 * one at a time, enforced by {@link CommandProcessor}'s global lock.
 *
 * Expiry is handled two ways, as Redis does:
 *  - lazily: an expired key is deleted when someone touches it;
 *  - actively: {@link #sweepExpired()} is called periodically in the background.
 */
public final class Store {
    public static final long NO_EXPIRY = -1;

    static final class Entry {
        Object value; // String or java.util.ArrayDeque<String> (list)
        long expiresAt; // epoch millis, or NO_EXPIRY

        Entry(Object value, long expiresAt) {
            this.value = value;
            this.expiresAt = expiresAt;
        }

        boolean isExpired(long now) {
            return expiresAt != NO_EXPIRY && now >= expiresAt;
        }
    }

    private final Map<String, Entry> data = new HashMap<>();
    private final Clock clock;

    public interface Clock {
        long millis();
    }

    public Store() {
        this(System::currentTimeMillis);
    }

    public Store(Clock clock) {
        this.clock = clock;
    }

    public long now() {
        return clock.millis();
    }

    /** Returns the live entry for key, deleting it first if it has expired. */
    Entry getEntry(String key) {
        Entry e = data.get(key);
        if (e == null) return null;
        if (e.isExpired(now())) {
            data.remove(key);
            return null;
        }
        return e;
    }

    void put(String key, Object value, long expiresAt) {
        data.put(key, new Entry(value, expiresAt));
    }

    boolean remove(String key) {
        Entry e = data.remove(key);
        return e != null && !e.isExpired(now());
    }

    int size() {
        sweepExpired();
        return data.size();
    }

    void clear() {
        data.clear();
    }

    List<String> keys(String globPattern) {
        Pattern p = globToRegex(globPattern);
        long now = now();
        List<String> out = new ArrayList<>();
        for (Map.Entry<String, Entry> e : data.entrySet()) {
            if (!e.getValue().isExpired(now) && p.matcher(e.getKey()).matches()) out.add(e.getKey());
        }
        return out;
    }

    /** A point-in-time copy of every live key, safe to serialize after the lock is released. */
    List<Rdb.Item> snapshot() {
        long now = now();
        List<Rdb.Item> items = new ArrayList<>(data.size());
        for (Map.Entry<String, Entry> e : data.entrySet()) {
            Entry en = e.getValue();
            if (en.isExpired(now)) continue;
            // Strings are immutable; lists are mutated in place, so copy them.
            Object v = en.value instanceof java.util.ArrayDeque<?> l ? new java.util.ArrayDeque<>(l) : en.value;
            items.add(new Rdb.Item(e.getKey(), v, en.expiresAt));
        }
        return items;
    }

    /** Replaces the keyspace with items loaded from disk, skipping ones that expired while we were down. */
    int load(List<Rdb.Item> items) {
        data.clear();
        long now = now();
        for (Rdb.Item item : items) {
            if (item.expiresAt() != NO_EXPIRY && now >= item.expiresAt()) continue;
            data.put(item.key(), new Entry(item.value(), item.expiresAt()));
        }
        return data.size();
    }

    /** Removes all expired keys. Returns how many were removed. */
    int sweepExpired() {
        long now = now();
        int removed = 0;
        for (Iterator<Entry> it = data.values().iterator(); it.hasNext(); ) {
            if (it.next().isExpired(now)) {
                it.remove();
                removed++;
            }
        }
        return removed;
    }

    /** Converts a Redis glob (*, ?, [abc], \x) into a regex. */
    static Pattern globToRegex(String glob) {
        StringBuilder re = new StringBuilder();
        for (int i = 0; i < glob.length(); i++) {
            char c = glob.charAt(i);
            switch (c) {
                case '*' -> re.append(".*");
                case '?' -> re.append('.');
                case '[' -> {
                    int end = glob.indexOf(']', i + 1);
                    if (end < 0) {
                        re.append("\\[");
                    } else {
                        // Redis and Java regex share [abc], [a-z] and [^abc] syntax.
                        re.append('[').append(glob, i + 1, end).append(']');
                        i = end;
                    }
                }
                case '\\' -> {
                    if (i + 1 < glob.length()) re.append(Pattern.quote(String.valueOf(glob.charAt(++i))));
                }
                default -> re.append(Pattern.quote(String.valueOf(c)));
            }
        }
        return Pattern.compile(re.toString(), Pattern.DOTALL);
    }
}
