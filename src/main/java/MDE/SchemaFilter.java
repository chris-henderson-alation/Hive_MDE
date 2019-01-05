package MDE;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class SchemaFilter {

    private final Set<String> whiteList;
    private final Set<String> blackList;

    public static final Collection<String> NO_RESTRICTIONS = null;

    public SchemaFilter(Collection<String> whiteList, Collection<String> blackList) {
        this.whiteList = whiteList == null ? new HashSet<>() : new HashSet<>(whiteList);
        this.blackList = blackList == null ? new HashSet<>() : new HashSet<>(blackList);
    }

    public boolean accepted(String schema) {
        return !this.blackList.contains(schema) && (this.whiteList.isEmpty() || this.whiteList.contains(schema));
    }
}
