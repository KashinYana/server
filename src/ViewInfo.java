import java.util.*;

public class ViewInfo
{
    public int view;
    public String primary;
    public String backup;

    public ViewInfo(ViewInfo other)
    {
        view = other.view;
        primary = other.primary;
        backup = other.backup;
    }

    public ViewInfo()
    {
        view = 0;
        primary = "";
        backup = "";
    }

    public ViewInfo(int view, String primary, String backup)
    {
        this.view = view;
        this.primary = primary;
        this.backup = backup;
    }
}
