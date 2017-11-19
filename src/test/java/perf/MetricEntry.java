package perf;

import java.util.Objects;

public class MetricEntry
{
    final int msgs;
    final int size;
    final int type;

    @Override
    public int hashCode()
    {
        return Objects.hash(msgs, size, type);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        MetricEntry other = (MetricEntry) obj;
        if (msgs != other.msgs) {
            return false;
        }
        if (size != other.size) {
            return false;
        }
        if (type != other.type) {
            return false;
        }
        return true;
    }

    public MetricEntry(int type, int msgs, int size)
    {
        this.type = type;
        this.msgs = msgs;
        this.size = size;
    }
}
