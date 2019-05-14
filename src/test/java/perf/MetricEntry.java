package perf;

public class MetricEntry
{
    final int msgs;
    final int size;
    final int type;

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + msgs;
        result = prime * result + size;
        result = prime * result + type;
        return result;
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
