import java.rmi.*;
import java.rmi.server.*;
import java.util.*;

public class Coordinator extends UnicastRemoteObject implements CoordinatorInterface
{
    public static int deadPings = 3;

    private long mCurrentTime;
    private TreeMap<String, Long> mServers;
    private HashMap<Integer, ViewInfo> mViewInfos;
    private int mCurrentViewId = 0;

    private boolean mIsLockedUpdateViewInfo = false;

    public Coordinator() throws RemoteException
    {
        mCurrentTime = 0;
        mServers = new TreeMap<String, Long>();
        mViewInfos = new HashMap<Integer, ViewInfo>();
        mViewInfos.put(0, new ViewInfo());
    }

    // this method is to be called by server
    public ViewInfo ping(int view, String serverName) throws RemoteException
    {
        checkThatCoordinatorAlive();

        ViewInfo viewInfo = mViewInfos.get(mCurrentViewId);
        if (isDead(viewInfo.primary) && isDead(viewInfo.backup) && mCurrentViewId != 0) {
            throw new RemoteException("Server were died.");
        }

        // new server or old server was rebooted
        if (view == 0) {
            mServers.put(serverName, mCurrentTime);
            if (serverName.equals(primary())) {
                swapPrimaryAndBackup();
            }
            return mViewInfos.get(mCurrentViewId);
        }

        // Primary confirm new view.
        if (mIsLockedUpdateViewInfo && serverName.equals(primary()) && view == mCurrentViewId) {
            mIsLockedUpdateViewInfo = false;
        }

        mServers.put(serverName, mCurrentTime);
        return mViewInfos.get(mCurrentViewId);
    }

    // this method is to be called by client
    public String primary() throws RemoteException
    {
        checkThatCoordinatorAlive();
        ViewInfo viewInfo = mViewInfos.get(mCurrentViewId);
        return viewInfo.primary;
    }

    void checkThatCoordinatorAlive() throws RemoteException {
        ViewInfo viewInfo = mViewInfos.get(mCurrentViewId);
        if (isDead(viewInfo.primary) && isDead(viewInfo.backup) && mCurrentViewId != 0) {
            throw new RemoteException("Server were died.");
        }
    }

    private String getPrimary()
    {
        ViewInfo viewInfo = mViewInfos.get(mCurrentViewId);
        return viewInfo.primary;
    }

    private String getBackup()
    {
        return mViewInfos.get(mCurrentViewId).backup;
    }

    // this method is to be called automatically as time goes by
    public void tick()
    {
        ++mCurrentTime;
        checkDeadServers();
    }

    private void checkDeadServers() {
        ViewInfo viewInfo = mViewInfos.get(mCurrentViewId);
        if (isDead(viewInfo.primary) && !isDead(viewInfo.backup) && !mIsLockedUpdateViewInfo) {
            // set backup to primary, find new back up
            String newBackUp = findNewBackUp();
            if (getBackup().isEmpty()) {
                return;
            }
            ViewInfo newViewInfo = new ViewInfo(mCurrentViewId + 1, getBackup(), newBackUp);
            mViewInfos.put(newViewInfo.view, newViewInfo);
            mCurrentViewId += 1;
            mIsLockedUpdateViewInfo = true;
        } else if (!isDead(viewInfo.primary) && isDead(viewInfo.backup) && !mIsLockedUpdateViewInfo) {
            // find new backup
            String newBackUp = findNewBackUp();
            ViewInfo newViewInfo = new ViewInfo(mCurrentViewId + 1, getPrimary(), newBackUp);
            mViewInfos.put(newViewInfo.view, newViewInfo);
            mCurrentViewId += 1;
            mIsLockedUpdateViewInfo = true;
        } else if (isDead(viewInfo.primary) && isDead(viewInfo.backup) && mCurrentViewId == 0) {
            String newPrimary = findNewBackUp();
            ViewInfo newViewInfo = new ViewInfo(mCurrentViewId + 1, newPrimary, getBackup());
            mViewInfos.put(newViewInfo.view, newViewInfo);
            mCurrentViewId += 1;
            mIsLockedUpdateViewInfo = true;
        } else if (isDead(viewInfo.primary) && isDead(viewInfo.backup)) {

        }
    }

    private void swapPrimaryAndBackup() {
        String newBackUp = getPrimary();
        if (getBackup().isEmpty()) {
            return;
        }
        ViewInfo newViewInfo = new ViewInfo(mCurrentViewId + 1, getBackup(), newBackUp);
        mViewInfos.put(newViewInfo.view, newViewInfo);
        mCurrentViewId += 1;
        mIsLockedUpdateViewInfo = true;
    }

    boolean isDead(String serverName) {
        if (serverName.isEmpty()) {
            return true;
        }
        return mServers.get(serverName) + deadPings < mCurrentTime;
    }

    private String findNewBackUp() {
        for(String nameServer : mServers.keySet()) {
            if (!isDead(nameServer)
                    && !nameServer.equals(getPrimary()) &&
                    !nameServer.equals(getBackup())) {
                return nameServer;
            }
        }
        return "";
    }

}
