//
// Created by Kave Salamatian on 26/11/2018.
//

#ifndef BGPGEOPOLITICS_BGPGRAPH_H
#define BGPGEOPOLITICS_BGPGRAPH_H
#include <iostream>                  // for std::cout
#include <stdio.h> 
#include <stdlib.h>
#include <thread>
#include <boost/graph/use_mpi.hpp>
//#include <boost/graph/distributed/mpi_process_group.hpp>
#include <boost/graph/use_mpi.hpp>
//#include <boost/graph/distributed/mpi_process_group.hpp>
#include <boost/iostreams/filtering_streambuf.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/copy.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/device/file_descriptor.hpp>
#include <boost/filesystem.hpp>
#include <boost/iostreams/device/file.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/bzip2.hpp>
#include <boost/graph/copy.hpp>
#include "tbb/concurrent_hash_map.h"
#include <boost/thread/shared_mutex.hpp>

#include <boost/graph/adjacency_list.hpp>
//#include <boost/property_map/property_map.hpp>
//#include <boost/graph/dijkstra_shortest_paths.hpp>
//#include <boost/graph/edge_list.hpp>
//#include <string>
#include <boost/property_map/property_map.hpp>
#include <boost/graph/graphml.hpp>
//#include <boost/numeric/ublas/matrix.hpp>
#include <boost/numeric/ublas/io.hpp>
//#include <boost/graph/floyd_warshall_shortest.hpp>
//#include <boost/graph/johnson_all_pairs_shortest.hpp>
#include <boost/graph/exterior_property.hpp>
//#include "cache.h"
#include "BGPGeopolitics.h"
#include "BlockingQueue.h"


using namespace std;
using namespace boost;
using namespace tbb;


struct VertexP {
    string asn;
    string country;
    string name;
    unsigned int time;
    int prefixNum;
    int prefixAll;
    int addNum;//number of Addresses
    int addAll; // number of overall Adresses
    int pathNum;
}; //bundled property map for nodes

struct EdgeP {
    int pathCount;
    int prefCount;
    int addCount;
    int weight;
    unsigned int time;
//    EdgeProperties(): count(0){}
//    EdgeProperties(int givencount): count(givencount){}
};

struct GraphP {
    string name;
};



typedef adjacency_list<setS, vecS, undirectedS, VertexP, EdgeP, GraphP> Graph;
typedef boost::graph_traits < Graph >::adjacency_iterator adjacency_iterator;
typedef boost::property_map<Graph, boost::vertex_index_t>::type IndexMap;

class BGPGraph{
public:
   Graph g;
//    Graph g1;
    dynamic_properties  dp;
    string outfile;
    MyThreadSafeMap<int,boost::graph_traits<Graph>::vertex_descriptor> asnToVertex;
    IndexMap index;
    
    BGPGraph(){}
    
    ~BGPGraph(){
        g.clear();
//        g1.clear();
    }


    
    Graph* copy(){
        Graph *g1=new Graph();
        copy_graph( g, *g1);
        return g1;
    }

    boost::graph_traits<Graph>::vertex_descriptor add_vertex(VertexP vertexP){
        boost::unique_lock<boost::shared_mutex> lock(mutex_);
        return boost::add_vertex(vertexP, g);
    }

    boost::graph_traits<Graph>::edge_descriptor  add_edge(boost::graph_traits<Graph>::vertex_descriptor v0, boost::graph_traits<Graph>::vertex_descriptor v1, EdgeP edgeP){
        auto p=boost::edge(v0,v1,g);
        if (p.second){
            set_edge(p.first,edgeP);
            return p.first;
        } else {
            boost::unique_lock<boost::shared_mutex> lock(mutex_);
            return boost::add_edge(v0, v1, edgeP, g).first;
        }
    }

    bool set_edge(boost::graph_traits<Graph>::edge_descriptor e, EdgeP edgeP){
        boost::unique_lock<boost::shared_mutex> lock(mutex_);
        g[e].pathCount = edgeP.pathCount;
        g[e].prefCount = edgeP.prefCount;
        g[e].addCount = edgeP.addCount;
        g[e].weight = edgeP.weight;
        g[e].time = edgeP.time;
        return false;
    }

    void set_vertex(boost::graph_traits<Graph>::vertex_descriptor v, VertexP vertexP){
        boost::unique_lock<boost::shared_mutex> lock(mutex_);
        g[v].prefixNum = vertexP.prefixNum;
        g[v].prefixAll = vertexP.prefixAll;
        g[v].addNum = vertexP.addNum;
        g[v].addAll = vertexP.addAll;
        g[v].pathNum = vertexP.pathNum;
        g[v].time = vertexP.time;
    }

    void get_vertex(boost::graph_traits<Graph>::vertex_descriptor v, VertexP &vertexP){
        boost::shared_lock<boost::shared_mutex> lock(mutex_);
        vertexP.asn = g[v].asn;
        vertexP.country = g[v].country;
        vertexP.name = g[v].name;
        vertexP.time = g[v].time;
        vertexP.prefixNum = g[v].prefixNum;
        vertexP.prefixAll = g[v].prefixAll;
        vertexP.addNum = g[v].addNum;
        vertexP.addAll = g[v].addAll;
        vertexP.pathNum = g[v].pathNum;

    }
    
    void remove_vertex(boost::graph_traits<Graph>::vertex_descriptor v){
        boost::unique_lock<boost::shared_mutex> lock(mutex_);
        boost::remove_vertex(v, g);
    }

    void remove_edge(boost::graph_traits<Graph>::edge_descriptor e){
        boost::unique_lock<boost::shared_mutex> lock(mutex_);
        if (boost::edge(source(e,g), target(e,g),g).second)
            boost::remove_edge(e, g);
    }

    void remove_edge(boost::graph_traits<Graph>::vertex_descriptor u, boost::graph_traits<Graph>::vertex_descriptor v){
        boost::unique_lock<boost::shared_mutex> lock(mutex_);
        if ((u!=-1) && (v!=-1)){
            if (boost::edge(u,v,g).second)
                boost::remove_edge(u,v,g);
        }
    }

    
    
    long  in_degree(boost::graph_traits<Graph>::vertex_descriptor v){
        boost::shared_lock<boost::shared_mutex> lock(mutex_);
        return boost::in_degree(v,g);
    }

    void clear(){
        g.clear();
    }
    
    pair<boost::graph_traits<Graph>::edge_descriptor, bool> edge(boost::graph_traits<Graph>::vertex_descriptor u, boost::graph_traits<Graph>::vertex_descriptor v){
        return boost::edge(u,v,g);
    }
    

private:
    mutable boost::shared_mutex mutex_;

};

class GraphToSave{
public:
    string outfile;
    Graph *g;
    GraphToSave(string outfile, Graph *g):outfile(outfile), g(g){}
    
};

class BGPSaver{
public:
    BlockingCollection<GraphToSave *> &graphsToSave;
    
    string outfile;
    MyThreadSafeMap<int,boost::graph_traits<Graph>::vertex_descriptor> asnToVertex;
    IndexMap index;

    BGPSaver(BlockingCollection<GraphToSave *> &graphsToSave): graphsToSave(graphsToSave){}
    
    dynamic_properties makeDP(Graph *g){
        dynamic_properties dp;
        dp.property("asNumber", get(&VertexP::asn, *g));
        dp.property("Country", get(&VertexP::country, *g));
        dp.property("Name", get(&VertexP::name, *g));
        dp.property("asTime", get(&VertexP::time, *g));
        dp.property("prefixNum", get(&VertexP::prefixNum, *g));
        dp.property("prefixAll", get(&VertexP::prefixAll, *g));
        dp.property("addNum", get(&VertexP::addNum, *g));
        dp.property("addAll", get(&VertexP::addAll, *g));
        dp.property("pathNum", get(&VertexP::pathNum, *g));
        dp.property("pathCount", get(&EdgeP::pathCount, *g));
        dp.property("prefCount", get(&EdgeP::prefCount, *g));
        dp.property("addCount", get(&EdgeP::addCount, *g));
        dp.property("edgeTime",get(&EdgeP::time, *g));
        dp.property("weight",get(&EdgeP::weight, *g));
        index = get(boost::vertex_index, *g);
        return dp;
    }
    
    void save(string outfile, Graph *g){
        boost::iostreams::filtering_ostream out;
        out.push(boost::iostreams::gzip_compressor());
        out.push( boost::iostreams::file_descriptor_sink(outfile+".gz"));
        write_graphml(out, *g,makeDP(g), true);
    }

    void run(){
        GraphToSave *p;
        while(true){
            graphsToSave.take(p);
            save(p->outfile, p->g);
            p->g->clear();
            delete p->g;
            delete p;
        }
    }

    
};


#endif //BGPGEOPOLITICS_BGPGRAPH_H
