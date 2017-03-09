package org.apache.giraph.factories;

import com.google.common.base.Objects;
import org.apache.giraph.comm.messages.SubgraphMessage;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.SubgraphId;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.Writable;

/**
 * Factory class to create default message values.
 *
 * @param <M> Message Value
 */
public class DefaultSubgraphMessageFactory<M extends Writable>
    implements MessageValueFactory<M> {
  /**
   * Message value class
   */
  private final Class<M> messageValueClass;
  /**
   * Configuration
   */
  private ImmutableClassesGiraphConfiguration conf;

  /**
   * Constructor
   *
   * @param messageValueClass message value class
   * @param conf              configuration
   */
  public DefaultSubgraphMessageFactory(Class<M> messageValueClass,
                                       ImmutableClassesGiraphConfiguration conf) {
    this.messageValueClass = messageValueClass;
    this.conf = conf;
  }

  @Override
  public M newInstance() {
    SubgraphMessage messageValue =  (SubgraphMessage) WritableUtils.createWritable(messageValueClass, conf);
    messageValue.initializeSubgraphId(conf.createSubgraphId());
    Writable subgraphMessageValue = conf.createSubgraphMessageValue();
    messageValue.initializeMessageValue(subgraphMessageValue);
    return (M) messageValue;
  }


  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("messageValueClass", messageValueClass)
        .toString();
  }
}