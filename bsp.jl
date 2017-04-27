import MPI

type Vertex
  vid
  value
  status  #active or inactive
  neighbors
end

type Neighbor
  vid
  gid       #location of this neighbor
  weight    #weight of edge
end

type Msg
  src
  dest
  value
end



rank = 0  #workerid
workern = 0  #number of workers

omsgqueue = Array(Any,workern)     #msg queue for every other workers
for i = 1:workern
  omsgqueue[i] = Array(Msg,0)
end

lomsgqueue = Array(Msg,0)       #msg queue for local vertices

vertexlist = 0                  #to be initialize later

msgqueues = 0                   #dictionary of msg queue for each local vertex. To be initialize later


# This function put msgs into corresponding queue to be deliver later
function sendmsg(src,neighbor::Neighbor,value)
  if neighbor.gid == rank   #target vertex is on local node
    push!(lomsgqueue,Msg(src,neighbor.vid,value))
  else                      #target vertex is on remote node
    push!(omsgqueue[neighbor.gid],Msg(src,neighbor.vid,value))
  end
end

# This function is used to determine if there are unread msgs for a given vertex
function msgdone(vid)
  if size(msgqueues[vid],1) != 0
    return false
  else
    return true
  end
end

# get msgs for a given vertex
function nextmsg(vid)
  if size(msgqueues[vid],1) != 0
    return pop!(msgqueues[vid])
  else
    return false
  end
end

# send msgs to all neighbors of a given vertex
function sendmsgtoallneighbors(vid,value)
  for each in vertexlist[vid].neighbors
    sendmsg(vid,each,value)
  end
end

# random partition function
# input:  array of edges;
#         [[v,u],
#           ...
#          [v,u]]
# ouput:  array of array of edges;
#         [[[v,u,gid of u],
#           [v,u,gid of u],
#           ......       ]],
#           ......         ]
function randomp(content)
  output = Array(Any,workern)
  for i = 1:workern
    output[i] = Array(Array{Int,1},0)
  end
  for each in content
    if each != ""
      each = split(each," ")
      a = parse(Int,each[1])
      b = parse(Int,each[2])
      index = a % workern
      item = [a,b,b % workern]
      push!(output[index + 1],item)
    end
  end
  return output
end

# Read graph into memory, initialize all global data
function preparegraphdata(graphpath)
  graphfile = open(graphpath,"r")
  content = readstring(graphfile)
  close(graphfile)
  content = split(content,'\n')
  dataofeachworker = randomp(content)
  #for each in dataofeachworker
  #  println("")
  #  println(each)
  #  println("")
  #end
  return dataofeachworker
end

MPI.Init()
rank = MPI.Comm_rank(MPI.COMM_WORLD)
workern = MPI.Comm_size(MPI.COMM_WORLD)
if rank == 0
  data = preparegraphdata("/Users/conghao/Documents/CS550/finalprj/testinput.txt")
  println(data)
  #transfer data to other workers
  vertexlist = copy(data[1])    #prepare local data
  for i = 2:workern                #transfer data to others
    #first convert data into char stream
    buffersize = 3 * size(data[i],1)
    buffer = Array(Char,buffersize)  #prepare buffer
    ii = 1
    for each in data[i]
      buffer[ii] = Char(each[1])
      buffer[ii + 1] = Char(each[2])
      buffer[ii + 2] = Char(each[3])
      ii = ii + 3
    end
    MPI.Send([buffersize],i - 1,0,MPI.COMM_WORLD)
    MPI.Send(buffer,i - 1,0,MPI.COMM_WORLD)
    sleep(3)
    println(vertexlist)
  end
elseif rank > 0
  buffersize = [0]
  MPI.Recv!(buffersize,0,0,MPI.COMM_WORLD)
  buffer = Array(Char,buffersize[1])
  MPI.Recv!(buffer,0,0,MPI.COMM_WORLD)
  #convert buffer from Char stream to array
  vertexlist = Array(Array{Int,1},0)
  i = 1
  while i < buffersize[1]
    src = Int(buffer[i])
    dest = Int(buffer[i + 1])
    gid = Int(buffer[i + 2])
    push!(vertexlist,[src,dest,gid])
    i += 3
  end
  println(vertexlist)
end
MPI.Finalize()
