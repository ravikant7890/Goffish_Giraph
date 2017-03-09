package org.apache.giraph.factories;

import org.apache.giraph.conf.GiraphConfigurationSettable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.Writable;

/**
 * Created by anirudh on 23/10/16.
 */
public class DefaultSubgraphVertexValueFactory<SVV extends Writable> implements SubgraphVertexValueFactory<SVV>, GiraphConfigurationSettable {

    private ImmutableClassesGiraphConfiguration conf;
    private Class<SVV> subgraphVertexValueClass;

    @Override
    public SVV newInstance() {
        return WritableUtils.createWritable(subgraphVertexValueClass, conf);
    }

    @Override
    public void setConf(ImmutableClassesGiraphConfiguration configuration) {
        this.conf = configuration;
        subgraphVertexValueClass = conf.getSubgraphVertexValueClass();
    }
}
