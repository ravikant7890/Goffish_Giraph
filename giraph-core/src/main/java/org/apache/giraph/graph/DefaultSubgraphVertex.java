package org.apache.giraph.graph;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;

/**
 * Created by anirudh on 27/09/16.
 */
public class DefaultSubgraphVertex<S extends WritableComparable, I extends WritableComparable,
        V extends Writable, E extends Writable, EI extends WritableComparable> implements SubgraphVertex<S, I, V, E, EI> {


    private I id;
    private V value;
    private LinkedList<SubgraphEdge<I, E, EI>> outEdges;

    @Override
    public LinkedList<SubgraphEdge<I, E, EI>> getOutEdges() {
        return outEdges;
    }

    @Override
    public int compareTo(Object o) {
        return 0;
    }

    @Override
    public I getId() {
        return id;
    }

    @Override
    public void setId(I id) {
        this.id = id;
    }

    @Override
    public V getValue() {
        return value;
    }

    @Override
    public void setValue(V value) {
        this.value = value;
    }

    @Override
    public boolean isRemote() {
        return false;
    }

    @Override
    public void initialize(I vertexId, V value, LinkedList<SubgraphEdge<I, E, EI>> edges) {
        this.id = vertexId;
        this.value = value;
        this.outEdges = edges;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
//        System.out.println("Write: " + "ID:" + id + id.getClass().getSimpleName());
//        System.out.println("Write: " + "Value:" + value + value.getClass().getSimpleName());
        id.write(dataOutput);
        value.write(dataOutput);
        int numOutEdges = outEdges.size();
        dataOutput.writeInt(numOutEdges);
//        System.out.println("Write: " + "Number edges: " + numOutEdges);
        for (SubgraphEdge<I, E, EI> edge : outEdges) {
//            System.out.println("Write: " + "Edge:" + edge.getSinkVertexId() + " Class: " + edge.getSinkVertexId().getClass().getSimpleName());
            edge.getSinkVertexId().write(dataOutput);
            edge.getValue().write(dataOutput);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        throw new NotImplementedException("Use read fields with ImmutableClassesGiraphConfiguration as parameter");
    }

    @Override
    public void readFields(ImmutableClassesGiraphConfiguration conf, DataInput dataInput) throws IOException {
        // Getting the subgraph vertex internals
        id = (I) conf.createSubgraphVertexId();
        value = (V) conf.createSubgraphVertexValue();

        id.readFields(dataInput);
        value.readFields(dataInput);

        //System.out.println("Read Subgraph ID:" + id + "\t"+ id.getClass().getSimpleName());
        //System.out.println("Read: " + "Value:" + value + value.getClass().getSimpleName());

        int numEdges;
        numEdges = dataInput.readInt();
        //System.out.println("Read: " + "Number edges: " + numEdges);
        outEdges = Lists.newLinkedList();
        for (int i = 0; i < numEdges; i++) {
            //System.out.println("\n THIS IS I :  "+ i);
            DefaultSubgraphEdge<I, E, EI> se = new DefaultSubgraphEdge<>();
            I targetId = (I) conf.createSubgraphVertexId();
            E edgeValue = (E) conf.createSubgraphEdgeValue();
            targetId.readFields(dataInput);
            edgeValue.readFields(dataInput);
            se.initialize(null, edgeValue, targetId);
            //System.out.println("Read: " + "Edge:" + se.getSinkVertexId() + " Class: " + se.getSinkVertexId().getClass().getSimpleName());
            outEdges.add(se);
        }
    }




}
