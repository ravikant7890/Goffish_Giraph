package org.apache.giraph.factories;

import org.apache.hadoop.io.Writable;

/**
 * Created by anirudh on 23/10/16.
 */
public interface SubgraphValueFactory<SV extends Writable>
        extends ValueFactory<SV> {
}
