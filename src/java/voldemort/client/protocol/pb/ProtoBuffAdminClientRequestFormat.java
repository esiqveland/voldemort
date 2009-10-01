package voldemort.client.protocol.pb;

import com.google.protobuf.ByteString;
import org.apache.log4j.Logger;
import voldemort.VoldemortException;
import voldemort.client.protocol.RequestFormatType;
import voldemort.client.protocol.VoldemortFilter;
import voldemort.client.protocol.admin.AdminClientRequestFormat;
import voldemort.client.protocol.admin.filter.DefaultVoldemortFilter;
import voldemort.cluster.Node;
import voldemort.store.ErrorCodeMapper;
import voldemort.store.StoreUtils;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.socket.SocketAndStreams;
import voldemort.store.socket.SocketDestination;
import voldemort.store.socket.SocketPool;
import voldemort.utils.ByteArray;
import voldemort.utils.NetworkClassLoader;
import voldemort.utils.Pair;
import voldemort.versioning.Versioned;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Iterator;
import java.util.List;

/**
 * Protocol buffers implementation for {@link voldemort.client.protocol.admin.AdminClientRequestFormat}
 *
 * @author afeinber
 */
public class ProtoBuffAdminClientRequestFormat extends AdminClientRequestFormat {
    private final ErrorCodeMapper errorMapper;
    private final static Logger logger = Logger.getLogger(ProtoBuffAdminClientRequestFormat.class);
    private final SocketPool pool;
    private final NetworkClassLoader networkClassLoader;

    public ProtoBuffAdminClientRequestFormat(MetadataStore metadataStore, SocketPool pool) {
        super(metadataStore);
        this.errorMapper = new ErrorCodeMapper();
        this.pool = pool;
        this.networkClassLoader = new NetworkClassLoader(Thread.currentThread()
                                                               .getContextClassLoader());
    }

    /**
     * Updates Metadata at (remote) Node
     *
     * @param remoteNodeId Node id to update
     * @param key Key to update
     * @param value The metadata 
     * @throws VoldemortException
     */
    @Override
    public void doUpdateRemoteMetadata(int remoteNodeId, ByteArray key, Versioned<byte[]> value) {
        Node node = this.getMetadata().getCluster().getNodeById(remoteNodeId);
        SocketDestination destination = new SocketDestination(node.getHost(),
                node.getSocketPort(),
                RequestFormatType.ADMIN_PROTOCOL_BUFFERS);
        SocketAndStreams sands = pool.checkout(destination);

        try {
            StoreUtils.assertValidKey(key);
            DataOutputStream outputStream = sands.getOutputStream();
            DataInputStream inputStream = sands.getInputStream();

            ProtoUtils.writeMessage(outputStream,
                    VAdminProto.VoldemortAdminRequest.newBuilder()
                    .setType(VAdminProto.AdminRequestType.UPDATE_METADATA)
                    .setUpdateMetadata(VAdminProto.UpdateMetadataRequest.newBuilder()
                            .setKey(ByteString.copyFrom(key.get()))
                            .setVersioned(ProtoUtils.encodeVersioned(value)))
                    .build());

            outputStream.flush();

            VAdminProto.UpdateMetadataResponse.Builder response = ProtoUtils.readToBuilder(
                    inputStream, VAdminProto.UpdateMetadataResponse.newBuilder());
            if (response.hasError())
                throwException(response.getError());
        } catch (IOException e) {
            close(sands.getSocket());
            throw new VoldemortException(e);
        } finally {
            pool.checkin(destination, sands);
        }
    }

    /**
     * Get Metadata from (remote) Node
     *
     *
     *
     * @param nodeId
     * @param storesList
     * @throws VoldemortException
     */
    @Override
    public Versioned<byte[]> doGetRemoteMetadata(int remoteNodeId, ByteArray key) {
        Node node = this.getMetadata().getCluster().getNodeById(remoteNodeId);

        SocketDestination destination = new SocketDestination(node.getHost(),
                node.getSocketPort(),
                RequestFormatType.ADMIN_PROTOCOL_BUFFERS);
        SocketAndStreams sands = pool.checkout(destination);
        try {
            DataOutputStream outputStream = sands.getOutputStream();
            DataInputStream inputStream = sands.getInputStream();

            ProtoUtils.writeMessage(outputStream,
                    VAdminProto.VoldemortAdminRequest.newBuilder()
                    .setType(VAdminProto.AdminRequestType.GET_METADATA)
                    .setGetMetadata(VAdminProto.GetMetadataRequest.newBuilder()
                            .setKey(ByteString.copyFrom(key.get())))
                    .build());

            outputStream.flush();

            VAdminProto.GetMetadataResponse.Builder response = ProtoUtils.readToBuilder(
                    inputStream, VAdminProto.GetMetadataResponse.newBuilder());
            if (response.hasError())
                throwException(response.getError());
            return ProtoUtils.decodeVersioned(response.getVersion());

        } catch (IOException e) {
            close(sands.getSocket());
            throw new VoldemortException(e);
        } finally {
            pool.checkin(destination, sands);
        }
    }

    /**
     * provides a mechanism to do forcedGet on (remote) store, Overrides all
     * security checks and return the value. queries the raw storageEngine at
     * server end to return the value
     *
     * @param proxyDestNodeId
     * @param storeName
     * @param key
     * @return List<Versioned <byte[]>>
     */
    @Override
    public List<Versioned<byte[]>> doRedirectGet(int proxyDestNodeId, String storeName, ByteArray key) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    /**
     * update Entries at (remote) node with all entries in iterator for passed
     * storeName
     *
     * @param nodeId
     * @param storeName
     * @param entryIterator
     * @param filterRequest: <imp>Do not Update entries filtered out (returned
     *                       false) from the {@link VoldemortFilter} implementation</imp>
     * @throws VoldemortException
     * @throws IOException
     */
    @Override
    public void doUpdatePartitionEntries(int nodeId, String storeName, Iterator<Pair<ByteArray, Versioned<byte[]>>> entryIterator, VoldemortFilter filter) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    /**
     * streaming API to get all entries belonging to any of the partition in the
     * input List.
     *
     * @param nodeId
     * @param storeName
     * @param partitionList
     * @param filterRequest: <imp>Do not fetch entries filtered out (returned
     *                       false) from the {@link VoldemortFilter} implementation</imp>
     * @return
     * @throws VoldemortException
     */
    @Override
    public Iterator<Pair<ByteArray, Versioned<byte[]>>> doFetchPartitionEntries(int nodeId, String storeName, List<Integer> partitionList, VoldemortFilter filter) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    /**
     * Delete all Entries at (remote) node for partitions in partitionList
     *
     * @param nodeId
     * @param storeName
     * @param partitionList
     * @param filterRequest: <imp>Do not Delete entries filtered out (returned
     *                       false) from the {@link VoldemortFilter} implementation</imp>
     * @throws VoldemortException
     * @throws IOException
     */
    @Override
    public int doDeletePartitionEntries(int nodeId, String storeName, List<Integer> partitionList, VoldemortFilter filter) {
        Node node = this.getMetadata().getCluster().getNodeById(nodeId);

        SocketDestination destination = new SocketDestination(node.getHost(),
                node.getSocketPort(),
                RequestFormatType.ADMIN_PROTOCOL_BUFFERS);
        SocketAndStreams sands = pool.checkout(destination);

        try {
            Class cl = filter == null ? DefaultVoldemortFilter.class : filter.getClass();
            byte[] classBytes = networkClassLoader.dumpClass(cl);
            DataOutputStream outputStream = sands.getOutputStream();
            DataInputStream inputStream = sands.getInputStream();

            VAdminProto.VoldemortAdminRequest.Builder request = VAdminProto.VoldemortAdminRequest.newBuilder()
                    .setType(VAdminProto.AdminRequestType.DELETE_PARTITION_ENTRIES)
                    .setDeletePartitionEntries(VAdminProto.DeletePartitionEntriesRequest.newBuilder()
                            .addAllPartitions(partitionList)
                            .setFilter(VAdminProto.VoldemortFilter.newBuilder()
                                    .setName(cl.getName())
                                    .setData(ProtoUtils.encodeBytes(new ByteArray(classBytes))))
                            .setStore(storeName));
            ProtoUtils.writeMessage(outputStream, request.build());
            outputStream.flush();

            VAdminProto.DeletePartitionEntriesResponse.Builder response = ProtoUtils.readToBuilder(inputStream,
                    VAdminProto.DeletePartitionEntriesResponse.newBuilder());
            if (response.hasError())
                throwException(response.getError());
            return response.getCount();
        } catch (IOException e) {
            close(sands.getSocket());
            throw new VoldemortException(e);
        } finally {
            pool.checkout(destination);
        }
    }

    public void throwException(VProto.Error error) {
            throw errorMapper.getError((short) error.getErrorCode(), error.getErrorMessage());
    }

    private void close(Socket socket) {
        try {
            socket.close();
        } catch(IOException e) {
            logger.warn("Failed to close socket");
        }
    }
}
