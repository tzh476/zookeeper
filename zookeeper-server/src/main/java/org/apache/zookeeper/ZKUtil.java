package org.apache.zookeeper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.common.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZKUtil {
    private static final Logger LOG = LoggerFactory.getLogger(ZKUtil.class);
    
    public static void deleteRecursive(ZooKeeper zk, final String pathRoot)
        throws InterruptedException, KeeperException
    {
        PathUtils.validatePath(pathRoot);

        List<String> tree = listSubTreeBFS(zk, pathRoot);
        LOG.debug("Deleting " + tree);
        LOG.debug("Deleting " + tree.size() + " subnodes ");
        for (int i = tree.size() - 1; i >= 0 ; --i) {
                        zk.delete(tree.get(i), -1);         }
    }


    
    public static void deleteRecursive(ZooKeeper zk, final String pathRoot, VoidCallback cb,
        Object ctx)
        throws InterruptedException, KeeperException
    {
        PathUtils.validatePath(pathRoot);

        List<String> tree = listSubTreeBFS(zk, pathRoot);
        LOG.debug("Deleting " + tree);
        LOG.debug("Deleting " + tree.size() + " subnodes ");
        for (int i = tree.size() - 1; i >= 0 ; --i) {
                        zk.delete(tree.get(i), -1, cb, ctx);         }
    }

    
    public static List<String> listSubTreeBFS(ZooKeeper zk, final String pathRoot) throws
        KeeperException, InterruptedException {
        Deque<String> queue = new LinkedList<String>();
        List<String> tree = new ArrayList<String>();
        queue.add(pathRoot);
        tree.add(pathRoot);
        while (true) {
            String node = queue.pollFirst();
            if (node == null) {
                break;
            }
            List<String> children = zk.getChildren(node, false);
            for (final String child : children) {
                final String childPath = node + "/" + child;
                queue.add(childPath);
                tree.add(childPath);
            }
        }
        return tree;
    }

    
    public static void visitSubTreeDFS(ZooKeeper zk, final String path, boolean watch,
        StringCallback cb) throws KeeperException, InterruptedException {
        PathUtils.validatePath(path);

        zk.getData(path, watch, null);
        cb.processResult(Code.OK.intValue(), path, null, path);
        visitSubTreeDFSHelper(zk, path, watch, cb);
    }

    @SuppressWarnings("unchecked")
    private static void visitSubTreeDFSHelper(ZooKeeper zk, final String path,
        boolean watch, StringCallback cb)
            throws KeeperException, InterruptedException {
                final boolean isRoot = path.length() == 1;
        try {
            List<String> children = zk.getChildren(path, watch, null);
            Collections.sort(children);

            for (String child : children) {
                String childPath = (isRoot ? path : path + "/") + child;
                cb.processResult(Code.OK.intValue(), childPath, null, child);
            }

            for (String child : children) {
                String childPath = (isRoot ? path : path + "/") + child;
                visitSubTreeDFSHelper(zk, childPath, watch, cb);
            }
        }
        catch (KeeperException.NoNodeException e) {
                                    return;         }
    }
}