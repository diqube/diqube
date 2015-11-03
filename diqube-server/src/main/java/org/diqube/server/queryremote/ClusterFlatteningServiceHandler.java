package org.diqube.server.queryremote;

import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.diqube.context.AutoInstatiate;
import org.diqube.remote.base.thrift.RNodeAddress;
import org.diqube.remote.cluster.thrift.ClusterFlatteningService;
import org.diqube.remote.cluster.thrift.RFlattenException;

/**
 * Handler for {@link ClusterFlatteningService}, which handles flattening local tables.
 *
 * @author Bastian Gloeckle
 */
@AutoInstatiate
public class ClusterFlatteningServiceHandler implements ClusterFlatteningService.Iface {

  @Override
  public void flattenAllLocalShards(String tableName, String flattenBy, List<RNodeAddress> otherFlatteners,
      RNodeAddress resultAddress) throws RFlattenException, TException {
  }

  @Override
  public void shardsFlattened(String tableName, String flattenBy,
      Map<Long, Long> origShardFirstRowIdToFlattenedNumberOfRows, RNodeAddress flattener) throws TException {
  }

  @Override
  public void flatteningDone(String tableName, String flattenBy, RNodeAddress flattener) throws TException {
  }

}
