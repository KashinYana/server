import java.rmi.*;
import java.util.*;

public interface CoordinatorInterface extends Remote
{
    public ViewInfo ping(int viewNum, String name) throws RemoteException;
    public String primary() throws RemoteException;
}
