/*
mpicxx sssp_v3.cc -Wall -O3 -fopenmp -std=c++11 -o sssp_v3
srun -p batch -N 2 -n 2 ./sssp_v3 ./testcase/random_100.in ./testcase/random_100_sssp.out 2

*/
#include <cstdio>
#include <cstdlib>
#include <mpi.h>
#include <iostream>
#include <vector>
#include <queue>
#include <stdio.h>
#include "pthread.h"
#include <signal.h>

using namespace std;


MPI_Request req;

int tot_send=0;
int tot_receive=0;

enum{data_tag};
enum{first_token = -1, second_token = -2, stop_recv = -3, pass_to = -4};

typedef struct EDGE Edge;

typedef struct Vertex{
	int id;
	int cur_optimal;
	int group;
	vector<Edge*> out_edges;
}Vertex;

struct EDGE{
	int weight;
	Vertex* target;
};

typedef struct Work{
	int vertex_id;
	int new_len;
}Work;

bool pass_forward;
Vertex** vertex_list = NULL;
Edge** edge_list = NULL;
queue<pair<int,int>> work_queue; //pair<target,cur_optimal>
bool pass_to_forward = false;

int* my_vertice;
int oo;

pthread_mutex_t queue_mutex = PTHREAD_MUTEX_INITIALIZER;

void* recv_task(void* aa){
	int recv_work_buf[2];
    int id=*(int*)aa;
	//MPI_COMM!!!!!!!!!
	while(true){
		MPI_Recv(recv_work_buf, 2, MPI_INT, id, data_tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
		//update queue

		if(recv_work_buf[0]==stop_recv){
			break;
		}else{			
			pthread_mutex_lock(&queue_mutex);
			//update work_queue
			pair<int,int> new_work = make_pair(recv_work_buf[0],recv_work_buf[1]);
			//cout<<"recv work "<<new_work.first<<endl;
            
            // Record how many receive
            if(new_work.first>=0){
                tot_receive++;
                //cout<<"rank: "<<oo<<" recv: "<<tot_receive<<endl;
            }
            

            work_queue.push(new_work);
            
			pthread_mutex_unlock(&queue_mutex);
		}
	}
	pthread_exit(NULL);
}


int break_count=0;

int main(int argc, char *argv[]){
    
    
	/// Set MPI Environment
	int provided;
	MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
	int num_tasks, rank, num_threads = 12;
	
	MPI_Comm_size(MPI_COMM_WORLD, &num_tasks);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	//cout<<"num_tasks "<<num_tasks<<endl;
	cout.setf(ios::unitbuf);
	int total_vertices, total_edges;
    oo = rank;
    
	/// Read Input File
	FILE *input_file = fopen(argv[1], "rb");
	fread(&total_vertices,sizeof(int),1,input_file);
	//cout<<"num vertices "<<total_vertices<<endl;
	fread(&total_edges,sizeof(int),1,input_file);
	//cout<<"num edges "<<total_edges<<endl;
	
	vertex_list = new Vertex*[total_vertices];
	for(int i=0;i<total_vertices;++i){
		vertex_list[i] = new Vertex();
		vertex_list[i]->id = i;
		vertex_list[i]->cur_optimal = -1;
		vertex_list[i]->group = -1;
	}

	//naive grouping
	int exceed = total_vertices % num_tasks;
	int group_count[num_tasks];
	for(int i=0;i<num_tasks;i++){
		if(i<exceed){
			group_count[i] = total_vertices / num_tasks + 1;
		}else{
			group_count[i] = total_vertices / num_tasks;
		}
	}
	
	for(int i=0;i<total_vertices;++i){
		vertex_list[i]->group = i%num_tasks;
	}
	
	edge_list = new Edge*[total_edges];
	int source_id, dest_id, edge_weight;
	for(int i=0;i<total_edges;i++){
		edge_list[i] = new Edge();
		fread(&source_id,sizeof(int),1,input_file);
		fread(&dest_id,sizeof(int),1,input_file);
		fread(&edge_weight,sizeof(int),1,input_file);
		edge_list[i]->weight = edge_weight;
		edge_list[i]->target = vertex_list[dest_id];
		//cout<<source_id<<" "<<dest_id<<" "<<edge_weight<<endl;
		if(vertex_list[source_id]->group==rank)
			vertex_list[source_id]->out_edges.push_back(edge_list[i]);
	}
	fclose (input_file);

	
	//put source in queue
	if(rank==0){
		pair<int,int> source = make_pair(0,0); 
		work_queue.push(source);
	}
	
	pthread_t threads[num_threads];
	Work* next_work = new Work[num_threads];
	MPI_Barrier(MPI_COMM_WORLD);
	//cout<<"start cal\n";
    
	// create thread: Each process has it own queue for other process, 
    // from avoiding reflush buffer~
    //
    // EX：
    // now we have 4 process, than each process have three work_queue
    int aa[num_tasks], bb;
    if(num_tasks==1){
        aa[0]=0;
        pthread_create(&threads[0], NULL, recv_task, (void*)&aa[0]);
    }
    else if(num_tasks==2){
        if(rank==0){
            aa[0]=1;
        }
        else{
            aa[0]=0;
        }
        pthread_create(&threads[0], NULL, recv_task, (void*)&aa[0]);
    }
    else if(num_tasks==3){
        if(rank==0){
            aa[0]=1;
            aa[1]=2;
        }
        else if(rank==1){
            aa[0]=0;
            aa[1]=2;
        }
        else{
            aa[0]=0;
            aa[1]=1;
        }
        pthread_create(&threads[0], NULL, recv_task, (void*)&aa[0]);
        pthread_create(&threads[1], NULL, recv_task, (void*)&aa[1]);
    }
    else if(num_tasks==4){
        if(rank==0){
            aa[0]=1;
            aa[1]=2;
            aa[2]=3;
        }
        else if(rank==1){
            aa[0]=0;
            aa[1]=2;
            aa[2]=3;
        }
        else if(rank==2){
            aa[0]=0;
            aa[1]=1;
            aa[2]=3;
        }
        else{
            aa[0]=0;
            aa[1]=1;
            aa[2]=2;
        }
        pthread_create(&threads[0], NULL, recv_task, (void*)&aa[0]);
        pthread_create(&threads[1], NULL, recv_task, (void*)&aa[1]);
        pthread_create(&threads[2], NULL, recv_task, (void*)&aa[2]);
    }
    
	
	//start relaxation
	pair<int,int> cur_work;
	int required_thread = 0, is_finished = 1;
	int ring_count = 0;
	Vertex *cur_vertex = NULL, *cur_target = NULL;
	bool passing_tag1 = false;
	
    MPI_Barrier(MPI_COMM_WORLD);
    
	while(true){
		pthread_mutex_lock(&queue_mutex);
		if(work_queue.empty()==false){
            pass_to_forward == true;
			pthread_mutex_unlock(&queue_mutex);
			//if(rank==1)
				//cout<<"go to work"<<endl;
			pthread_mutex_lock(&queue_mutex);
			cur_work = work_queue.front();
			work_queue.pop();
			pthread_mutex_unlock(&queue_mutex);
			if(cur_work.first==first_token){
				///dual ring tag 1
				//cout<<"recv tag1"<<endl;
				if(rank==0){
					///pass one round
                    break_count++;
                    
                    passing_tag1 = false;
                    //if(break_count%20==0)
                        //cout<<"break iter: "<<break_count<<endl;
                    pass_to_forward = false;
                    if(break_count==500){
                        
                        //pthread_mutex_lock(&queue_mutex);
                        int send_work_buf[2] = {second_token,cur_work.second};
                        MPI_Send(send_work_buf, 2, MPI_INT, 1, data_tag, MPI_COMM_WORLD);
                        //pthread_mutex_unlock(&queue_mutex);
                        
                        passing_tag1 = false; 
                        if(cur_work.second==0){
                            if(!work_queue.empty())
                                cout<<"rank:   "<<rank<<"  RRRRRRRRRRRRace CCCCCCondition queue size: "<<work_queue.size()<<endl;
                            break;
                        }
                    }
                    else{
                        
                    }
				}else{
					int send_work_buf[2] = {first_token,cur_work.second};
					if(pass_to_forward == true || work_queue.empty()==false || cur_work.second==1)
						send_work_buf[1] = 1;
					if(rank == num_tasks-1)
						MPI_Send(send_work_buf, 2, MPI_INT,      0, data_tag, MPI_COMM_WORLD);
					else
						MPI_Send(send_work_buf, 2, MPI_INT, rank+1, data_tag, MPI_COMM_WORLD);
				}
				///refresh pass to forward
				pass_to_forward = false;
			}else if(cur_work.first==second_token){
				///dual ring tag 2
				///decide terminate
                if(!work_queue.empty())
                    cout<<"rank:   "<<rank<<"  RRRRRRRRRRRRace CCCCCCondition queue: "<<work_queue.size()<<endl;
                    
				int send_work_buf[2] = {second_token,cur_work.second};
				if(rank != num_tasks-1)
					MPI_Send(send_work_buf, 2, MPI_INT, rank+1, data_tag, MPI_COMM_WORLD);
				if(cur_work.second==0){
                    
					break;
                }
			}else{
				//relaxlation
				cur_vertex = vertex_list[cur_work.first];
				if( cur_vertex->cur_optimal == -1 || cur_vertex->cur_optimal > cur_work.second){
                    
                    
					
                    cur_vertex->cur_optimal = cur_work.second;
					//cout<<rank<<" update :"<<vertex_list[cur_work.first]->cur_optimal<<endl;
					for(int i=0;i<cur_vertex->out_edges.size();++i){
						cur_target = cur_vertex->out_edges[i]->target;
						if(cur_target->group==cur_vertex->group){//target is in group
							pthread_mutex_lock(&queue_mutex);
							//update work_queue
							int new_target = cur_target->id;
							int new_len  =  cur_vertex->cur_optimal + cur_vertex->out_edges[i]->weight;
							pair<int,int> new_work = make_pair(new_target,new_len);
							work_queue.push(new_work);
							pthread_mutex_unlock(&queue_mutex);
							//cout<<"out "<<i<<endl;
						}else{
							//do MPI_Send
							//MPI_COMM!!!!!!!!!
							int send_work_buf[2];
							send_work_buf[0] = cur_target->id;
							send_work_buf[1] = cur_vertex->cur_optimal + cur_vertex->out_edges[i]->weight;
							//cout<<"send to "<<cur_target->group<<"...";
                            tot_send++;
                            //if(tot_send>6000)
                                //cout<<"rank: "<<rank<<" send: "<<tot_send<<endl;
							MPI_Send(send_work_buf, 2, MPI_INT, cur_target->group, data_tag, MPI_COMM_WORLD);
							//cout<<"... finish send"<<cur_target->group<<endl;
							//if(cur_target->group<cur_vertex->group){
							pass_to_forward = true;
							//}
						}
					}
				}
			}
		}else{
			pthread_mutex_unlock(&queue_mutex);
			if(rank==0){
				if(num_tasks==1){
					break;
				}else if(passing_tag1==false){
					///send tag1
					int send_work_buf[2] = {first_token,0};
					//cout<<"send tag1"<<"{"<<send_work_buf[0]<<" , "<<send_work_buf[1]<<"}"<<endl;
					MPI_Send(send_work_buf, 2, MPI_INT, 1, data_tag, MPI_COMM_WORLD);
					passing_tag1= true;
				}
			}else{
				//cout<<work_queue.size()<<endl;
			}
		}
	}
	//cout<<"all done\n"<<endl;
    MPI_Barrier(MPI_COMM_WORLD);
	int a[2] = {stop_recv,-1};
    if(num_tasks==1 || num_tasks==2){
        MPI_Send(a , 2, MPI_INT, aa[0], data_tag, MPI_COMM_WORLD);
        pthread_join(threads[0], NULL);
    }
    else if(num_tasks==3){
        MPI_Send(a , 2, MPI_INT, aa[0], data_tag, MPI_COMM_WORLD);
        MPI_Send(a , 2, MPI_INT, aa[1], data_tag, MPI_COMM_WORLD);
        pthread_join(threads[0], NULL);
        pthread_join(threads[1], NULL);
    }
    else if(num_tasks==4){
        MPI_Send(a , 2, MPI_INT, aa[0], data_tag, MPI_COMM_WORLD);
        MPI_Send(a , 2, MPI_INT, aa[1], data_tag, MPI_COMM_WORLD);
        MPI_Send(a , 2, MPI_INT, aa[2], data_tag, MPI_COMM_WORLD);
        pthread_join(threads[0], NULL);
        pthread_join(threads[1], NULL);
        pthread_join(threads[2], NULL);
    }
	

	int* gobal_result = new int[total_vertices];
	int my_data_size = group_count[rank];
	int* my_ans = new int[my_data_size];
	int cur_position = 0;
	int minimun_workload = total_vertices/num_tasks;
	int displs[num_tasks];
	displs[0] = 0;
    
    MPI_Barrier(MPI_COMM_WORLD);
    // write result into file
    MPI_File outputFile;
	MPI_File_open(MPI_COMM_WORLD, argv[2], MPI_MODE_CREATE | MPI_MODE_WRONLY, MPI_INFO_NULL, &outputFile);
    //offset = 4 * begin * vertexNum;
    MPI_Offset offset;
    for(int i=0;i<total_vertices;i++)
        if(vertex_list[i]->group==rank){
            
            offset= i *sizeof(int);
            //cout<<vertex_list[i]->id<<" "<<vertex_list[i]->cur_optimal<<endl;
            MPI_File_write_at(outputFile, offset , &vertex_list[i]->cur_optimal, 1, MPI_INT, MPI_STATUS_IGNORE);
        }
	MPI_File_close(&outputFile);
    
    
    
	/*for(int i=1;i<num_tasks;i++){
		displs[i] = displs[i-1]+group_count[i-1];
	}
	int k = 0;
	for(int i=0;i<total_vertices;i++){
		if(rank==vertex_list[i]->group){
			my_ans[k] = vertex_list[i]->cur_optimal;
			k++;
		}
	}*/
//    MPI_Gatherv(const void *sendbuf, int sendcount, MPI_Datatype sendtype, void *recvbuf, const int *recvcounts, 
//				const int *displs, MPI_Datatype recvtype, int root, MPI_Comm comm)
    /*MPI_Gatherv( my_ans, my_data_size, MPI_INT, gobal_result, group_count, 
				 displs, MPI_INT, 0, MPI_COMM_WORLD);
	
	int *output = new int[total_vertices];
	for(int i=0;i<num_tasks;i++){
		for(int j=0;j<group_count[i];j++){
			output[i+j*num_tasks] = gobal_result[displs[i]+j];
		}
	}*/
	
    //cout<<"rank: "<<rank<<" send: "<<tot_send<<" recv: "<<tot_receive<<endl;
    /*if(rank==0){
	FILE * pFile = fopen (argv[2], "wb");
	fwrite (output , sizeof(int), total_vertices, pFile);
	fclose (pFile);
    }*/
	//if(rank==0){
        /*FILE *output_file = fopen( argv[3] , "rb");
        int ans1[total_vertices]={0};
        for(int i=0; i<total_vertices; ++i){
            int src2;
            fread(&src2, sizeof(int), 1, output_file);
            ans1[i]=src2;
        
        }
        cout<<"wrong vertecs:"<<endl;
		for(int i=0;i<total_vertices;++i){
            if(vertex_list[i]->group==rank && ans1[i]!=vertex_list[i]->cur_optimal)
                cout<<ans1[i]<<" "<<vertex_list[i]->cur_optimal<<endl;
                //cout<<"fuck you"<<endl;
		}
        cout<<"end of wrong vertecs"<<endl;*/
	//}
	//free all
	for(int i=0;i<total_edges;i++){
		delete[] edge_list[i];
	}
	for(int i=0;i<total_vertices;i++){
		delete vertex_list[i];
	}
	delete[] vertex_list;
	delete[] edge_list;
	delete[] next_work;
	delete[] my_vertice;
	delete[] gobal_result;
	delete[] my_ans;
	MPI_Finalize();

	return 0;
	
}


