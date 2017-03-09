package org.apache.giraph.examples;

/**
 * Created by anirudh on 27/09/16.
 */
public class SubgraphVertexCount {
}
//extends SubgraphComputation<WritableComparable, Writable, Writable> {
//    private long currentCount = 0;
//    @Override
//    public void compute(Subgraph subgraph, Iterable messages) throws IOException {
//        SubgraphVertices sv = subgraph.getValue();
//
//        if(subGraphMessages == null || subGraphMessages.size() == 0) {
//            currentCount = sv.getNumVertices();
//
//            // TODO: getEdges should do the same thing
//            for(SubgraphVertex vertex : subgraph.getRemoteVertices()) {
//                currentCount--;
//            }
//
////            System.out.println(partition.getVId());
//            // TODO: A reference to all subgraphs
////            for(long part : partitions) {
////                String count = "" + partition.getVId() + ":" + subgraph.getVId() + ":" + currentCount;
////                SubGraphMessage msg = new SubGraphMessage(count.getBytes());
////                sendMessage(part,msg);
////            }
//
//        } else {
//
//            for(SubGraphMessage msg : subGraphMessages) {
//
//                String count = new String(msg.getData());
//                String dataParts[] = count.split(":");
//
//                long partId = Long.parseLong(dataParts[0]);
//                long subId = Long.parseLong(dataParts[1]);
//
//                if(!(partition.getId() == partId && subgraph.getId() == subId)) {
//                    currentCount += Integer.parseInt(dataParts[2]);
//                }
//
//            }
//
//
//            try {
//                File file = new File("vert-count.txt");
//                PrintWriter writer = new PrintWriter(file);
//                writer.write("Total Vertex Count :" + currentCount );
//                writer.flush();
//                writer.close();
//            } catch (FileNotFoundException e) {
//                e.printStackTrace();
//            }
//            voteToHalt();
//        }
//    }
//
//
//        @Override
//        public void compute(List<SubGraphMessage> subGraphMessages) {
//
//        }
//
//        public void reduce(List<SubGraphMessage> messageList) {
//            try {
//                File file = new File("vert-count-reduce.txt");
//                PrintWriter writer = new PrintWriter(file);
//                writer.write("Total Vertex Count :" + currentCount );
//                writer.flush();
//                writer.close();
//            } catch (FileNotFoundException e) {
//                e.printStackTrace();
//            }
//            voteToHalt();
//        }
//    }
//}
