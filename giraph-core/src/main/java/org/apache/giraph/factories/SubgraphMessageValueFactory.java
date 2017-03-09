package org.apache.giraph.factories;

import org.apache.hadoop.io.Writable;

/**
 * Created by anirudh on 29/11/16.
 */
public interface SubgraphMessageValueFactory<M extends Writable>
    extends ValueFactory<M> {
}
