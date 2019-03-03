usage="Usage: $0 [num_edges] [num_vertices]"
 if [ $# -le 0 ]; then
   echo $usage
   exit 1
 fi

GRAPHBENCH=`pwd`

NUM_E=$1
NUM_V=$2

a=0.5
b=0.1
c=0.1

$GRAPHBENCH/PaRMAT/Release/PaRMAT -nEdges $NUM_E -nVertices $NUM_V \
                                  -a $a -b $b -c $c \
                                  -noDuplicateEdges \
                                  -noEdgeToSelf \
                                  -output "$NUM_E"e_graph.txt
