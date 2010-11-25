package ida.ipl;

import java.io.IOException;
import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;

import ibis.ipl.*;

public class IdaNode{

	private static int total_threads;
	private static thread_routine[] threads;
	Board board_initial;
	private Ibis ibis;
	private Registry registry;
	private static boolean cache_flag;
	private static boolean queue_filled_flag;
	private static int solutions=0;
	private static int myId = -1;//Identification assigned to each node by the master
	private static Board myBoard;//Board assigned to each node by the master
	// total number of machines in the Ibis pool
	private static int total_machines=0;

	// Blocking queue for holding the jobs
	private static ArrayBlockingQueue<Object> queue;

	//cache used by the threads
	private static BoardCache[] thread_cache;

	private static BoardCache IplCache;
	// Ports used for ibis communication

	/* Master Port: used for broadcasting WORK_FLAGs to all worker nodes
	 * -1 flag: to TERMINATE all the workers
	 * 0 flag: to continue working
	 * one-to-many connections: since server will be connected to multiple workers
	 */	
	private PortType broadcast_port =
		new PortType(PortType.COMMUNICATION_RELIABLE,
				PortType.SERIALIZATION_DATA,
				PortType.RECEIVE_EXPLICIT,
				PortType.CONNECTION_ONE_TO_MANY);

	/* Worker Port: used for sending solutions back to master node
	 * many-to-one: since server will receive multiple solutions
	 */
	private PortType solution_port =
		new PortType(PortType.COMMUNICATION_RELIABLE,
				PortType.SERIALIZATION_DATA,
				PortType.RECEIVE_EXPLICIT,
				PortType.CONNECTION_MANY_TO_ONE);

	/* Master Port: used for sending assigned boards to worker nodes
	 * one-to-one: server will send board explicitly to each worker
	 */
	private PortType board_port =
		new PortType(PortType.COMMUNICATION_RELIABLE,
				PortType.COMMUNICATION_FIFO,
				PortType.SERIALIZATION_OBJECT,
				PortType.SERIALIZATION_OBJECT_IBIS,
				PortType.RECEIVE_EXPLICIT,
				PortType.CONNECTION_ONE_TO_ONE);

	/* Fixed number of total nodes */
	private IbisCapabilities ibisCapabilities =
		new IbisCapabilities(IbisCapabilities.ELECTIONS_STRICT,
				IbisCapabilities.CLOSED_WORLD,
				IbisCapabilities.MEMBERSHIP_TOTALLY_ORDERED);

	//port used by master to send information_messages to client
	private static SendPort send_info_port;

	/* define constructor */
	public IdaNode(int threads, Board board, boolean usecache) throws Exception{
		total_threads = threads;
		board_initial = board;
		cache_flag = usecache;
		queue_filled_flag = false;
		if(cache_flag)
		{
			thread_cache = new BoardCache[total_threads];
			IplCache = new BoardCache();
			for(int r=0; r<total_threads; r++)
				thread_cache[r]=new BoardCache();
		}
		/*Create an Ibis interface */
		ibis = IbisFactory.createIbis(ibisCapabilities, null, broadcast_port, solution_port, board_port);
	}

	public void run(int thr) throws Exception {
		registry = ibis.registry();

		/* initialise the threads */
		threads = new thread_routine[total_threads];
		/*elect a server*/
		IbisIdentifier server = registry.elect("master");

		if (server.equals(ibis.identifier())) {
			System.out.println("Running Ida with "+thr+" threads \n");

			long[] result = new master().run();
			System.out.println("\nresult is " + result[0] + " solutions of "
					+ result[1] + " steps");
			System.err.println("ida took " + result[2] + " milliseconds");
		}
		else {
			new worker(server).run();
		}
		ibis.end();
	}

	/* synchronously adding the number of solutions found */
	private synchronized static void addSolutions(int result) {
		IdaNode.solutions += result;
	}

	/* This will spawn a master for this puzzle
	 * which will handle and coordinate between
	 * various worker machines 
	 */
	public class master{
		int bound;

		// IDs to be assigned to the nodes;
		int assigned_ids=1;
		/* receiving port on master:
		 * - to receive solutions from workers
		 * - to receive information messages (send by itself)
		 */
		ReceivePort receive_solution_port, receive_info_port;

		/* send port on master:
		 * - to send tasks(boards) to workers
		 * - to send information messages to workers (and itself)
		 */
		SendPort send_board_port;


		IbisIdentifier[] joinedIbises;
		long time_taken;

		public master() throws IOException {
			solutions = 0;
			bound = board_initial.distance();

			/* receive ports of master*/
			//receive_solution_port = ibis.createReceivePort(solution_port, "solution", new IdaNode.solutionUpcall());
			//receive_info_port = ibis.createReceivePort(broadcast_port, "info", new IdaNode.infoUpcall());

			receive_solution_port = ibis.createReceivePort(solution_port, "solution");
			receive_info_port = ibis.createReceivePort(broadcast_port, "info");

			//enable connection
			receive_solution_port.enableConnections();
			//receive_solution_port.enableMessageUpcalls();

			receive_info_port.enableConnections();
			//receive_info_port.enableMessageUpcalls();

			/*send port of master*/
			send_info_port = ibis.createSendPort(broadcast_port);
			System.out.println("server ports created\n");//DEBUG
		}

		public long[] run() throws IOException
		{
			long total_time_taken=0;

			/* result array to collect:
			 * [0]- number of solutions
			 * [1]- bound value
			 * [2]- Time (in milliseconds)
			 */
			long[] result = new long[3];

			/* now wait for all the machines
			 * to join the Ibis pool
			 */
			registry.waitUntilPoolClosed();

			/* stores the identifiers
			 * of the machines in the pool
			 */
			joinedIbises = registry.joinedIbises();
			total_machines = joinedIbises.length;

			/* Now connect the information broadcast
			 * port to all the machines(including) master
			 * in ibis pool
			 */
			for(int i=0; i<total_machines; i++)
			{
				send_info_port.connect(joinedIbises[i], "info");
			}
			System.out.println("Server Info port connected to IbisNodes");//DEBUG

			System.out.print("Try bound ");
			System.out.flush();

			/* Boards port will be used for sending the nodes Ids (since it is one-to-one)
			 * 1. Connect the port to each node(except master) and write the ID
			 * 2. finish the message and close the port
			 */
			for(int i=0; i<total_machines; i++)
			{
				if (!(joinedIbises[i].equals(ibis.identifier())))
				{
					send_board_port = ibis.createSendPort(board_port);
					send_board_port.connect(joinedIbises[i], "board");
					WriteMessage m = send_board_port.newMessage();
					m.writeInt(assigned_ids);
					System.out.println("machine "+i+" has id: "+assigned_ids);//DEBUG
					m.finish();
					send_board_port.disconnect(joinedIbises[i], "board");
					send_board_port.close();
					assigned_ids++;
				}
			}
			System.out.println("machines Ids sent");//DEBUG
			/* 1. Create the port to send the initial board to each node (except master)
			 * 2. Connect the port to each node and write the board
			 * 3. Finish the message and close the board port
			 */

			for(int i=0; i<total_machines; i++)
			{
				if (!(joinedIbises[i].equals(ibis.identifier())))
				{
					send_board_port = ibis.createSendPort(board_port);
					send_board_port.connect(joinedIbises[i], "board");
					WriteMessage m = send_board_port.newMessage();
					m.writeObject(board_initial);
					System.out.println("Board sent to machine: "+i);//DEBUG
					m.finish();
					send_board_port.disconnect(joinedIbises[i], "board");
					send_board_port.close();
				}
			}
			System.out.println("Boards sent");//DEBUG
			
			/* Now send the total number of machines which joined tha pool
			 * to each member
			 */

			WriteMessage msg_total_machines = send_info_port.newMessage();
			msg_total_machines.writeInt(total_machines);
			msg_total_machines.finish();
			
			while(true)
			{
				/* With every bound iteration, check 
				 * whether in previous bound solutions were
				 * found or not
				 */

				if(IdaNode.solutions !=0)
				{
					/* Yes, tell all worker nodes to TERMINATE */
					WriteMessage m = send_info_port.newMessage();
					m.writeInt(-1);
					m.finish();
					break;
				}

				else
				{
					/*give the signal to worker to CONTINUE*/
					WriteMessage m = send_info_port.newMessage();
					m.writeInt(0);
					m.finish();
				}

				/*Initialising the job queue with:
				 * capacity = capacity of board cache = 10*1024
				 * FIFO = ON 
				 */
				queue = new ArrayBlockingQueue<Object>(10*1024, true);

				/* start the threads
				 * Threads will not start immediately, 
				 * since they will wait for notification from master thread
				 * that jobs creation has been initiated 
				 */
				for(int i=0;i<total_threads; i++){
					threads[i]=new thread_routine();
					String astring = Integer.toString(i);
					/* setting the name of the thread to integer number */ 
					threads[i].setName(astring);
					threads[i].start();
				}

				//start the timer 
				long start = System.currentTimeMillis();

				board_initial.setBound(bound);
				//Master assigns itself Id as 0
				myId = 0;

				/* now master will generate the board positions 
				 * from the initial board and fill the job queue
				 * for its threads. Job submission in the queue
				 * will be according to the Id assigned
				 */
				generate_boards(board_initial);

				System.out.println("queue filled up by Server"); //DEBUG
				/* wait for all threads to finish */
				for(int i=0; i<total_threads; i++)
				{
					try {
						threads[i].join();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}

				System.out.println("Server threads finished");//DEBUG
				/* wait for all other nodes to report back the results */
				for(int i=1; i<total_machines; i++)
				{
					ReadMessage worker_solutions = receive_solution_port.receive();
					int child_sol = worker_solutions.readInt();
					worker_solutions.finish();
					IdaNode.addSolutions(child_sol);
				}
				System.out.println("received solutions from all workers");//DEBUG
				long end = System.currentTimeMillis();
				time_taken +=(end-start);
				bound+=2;
			}//while block ends here

			bound -=2;
			result[0]=IdaNode.solutions;
			result[1]=bound;
			result[2]=time_taken;

			/* Close all the ports*/
			send_info_port.close();
			receive_solution_port.close();
			send_board_port.close();
			receive_info_port.close();
			return result;
		}//run ends here
	}//master class ends here

	/* This will spawn a worker on each worker machine
	 * and will solve the jobs put up by master
	 */
	public class worker{
		IbisIdentifier master;

		/* client ports */
		/* receiving ports on client machine:
		 * - to receive information_message from master
		 * - to receive board from master
		 */
		ReceivePort receive_board_port, receive_info_port;
		/* send port on client machine:
		 * - to send solutions back to master
		 */
		SendPort send_solution_port;
		int info_msg;
		int bound;
		public worker(IbisIdentifier server) throws IOException {
			master = server;
			send_solution_port = ibis.createSendPort(solution_port);
			send_solution_port.connect(master, "solution");

			//receive_board_port = ibis.createReceivePort(board_port, "board", new IdaNode.boardUpcall());
			receive_board_port = ibis.createReceivePort(board_port, "board");
			receive_board_port.enableConnections();
			//receive_board_port.enableMessageUpcalls();

			//receive_info_port = ibis.createReceivePort(broadcast_port, "info", new IdaNode.infoUpcall());
			receive_info_port = ibis.createReceivePort(broadcast_port, "info");
			receive_info_port.enableConnections();
			//receive_info_port.enableMessageUpcalls();
			System.out.println("worker ports created\n");//DEBUG
		}
		public void run() throws IOException, InterruptedException {

			/* first receive the machineID */
			ReadMessage msg_id = receive_board_port.receive();
			if(myId==-1)
				myId = msg_id.readInt();
			msg_id.finish();
			System.out.println("received myID");//DEBUG

			/* now receive the board */
			ReadMessage msg_board = receive_board_port.receive();
			if(myBoard==null)
			{
				try {
					myBoard = (Board) msg_board.readObject();
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			msg_board.finish();
			System.out.println("received myBoard");//DEBUG
			
			/* receive the total_machines from Server */
			ReadMessage msg_total_machines = receive_info_port.receive();
			IdaNode.total_machines = msg_total_machines.readInt();
			msg_total_machines.finish();
			System.out.println("received total_machines: "+total_machines);
			
			bound = myBoard.distance();
			while(true)
			{
				info_msg = 0;
				/* receive the information_message from master
				 * to confirm whether to continue or not
				 */
				ReadMessage msg_info = receive_info_port.receive();
				info_msg = msg_info.readInt();
				msg_info.finish();

				/* TERMINATE signal */
				if(info_msg == -1)
					break;

				/* CONTINUE signal */
				else
				{
					/*Initialising the job queue with:
					 * capacity = capacity of board cache = 10*1024
					 * FIFO = ON 
					 */
					queue = new ArrayBlockingQueue<Object>(10*1024, true);
					
					/* start the threads
					 * Threads will not start immediately, 
					 * since they will wait for notification from master thread
					 * that jobs creation has been initiated 
					 */
					for(int i=0;i<total_threads; i++){
						threads[i]=new thread_routine();
						String astring = Integer.toString(i);
						/* setting the name of the thread to integer number */ 
						threads[i].setName(astring);
						threads[i].start();
					}
					myBoard.setBound(bound);
					//expand the board
					generate_boards(myBoard);
					System.out.println("queue filled up by worker");//DEBUG
					
					/* wait for all threads to finish */
					for(int i=0; i<total_threads; i++)
					{
						try {
							threads[i].join();
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					System.out.println("Worker threads finished");//DEBUG
					
					WriteMessage msg_solution = send_solution_port.newMessage();
					msg_solution.writeInt(solutions);
					msg_solution.finish();
					System.out.println("Solution sent to server");//DEBUG
					bound +=2;
				}//else block end here
			}//while block ends here
			/* close all the ports */
			send_solution_port.close();
			receive_info_port.close();
			receive_board_port.close();
		}//run ends here

	}//worker class ends here

	public void generate_boards(Board board) {
		LinkedList<Board> board_list = new LinkedList<Board>();
		Board child_board = null;
		Board my_board = null;
		int board_identifier;
		board_list.addLast(board);

		/* now fill the list until we have pre-defined number of boards */
		while(board_list.size()< 4000)
		{
			if(board_list.size()==0)
				break;
			child_board = board_list.getFirst();
			/* now perform some optimization checks */
			if(child_board.distance()==0)
			{
				addSolutions(1);
				continue;
			}

			if(child_board.distance()>board.bound())
				continue;

			/* 1. now expand the baord 
			 * 2. Add the boards to the linked list
			 */
			Board[] children_array = null;
			if(cache_flag)
				children_array = child_board.makeMoves(IdaNode.IplCache);
			else
				children_array = child_board.makeMoves();

			for(int i=0;i<children_array.length; i++)
			{
				if(children_array[i]!=null)
				{
					board_list.addLast(children_array[i]);
				}
			}
		}//while block ends here	
		/* Now comes the main part of getting the boards intended for particular node
		 * which will depend on the Id assigned to each machine
		 */
		for(int i=0; i<board_list.size(); i+=total_machines)
		{
			board_identifier = i + (myId % total_machines);
			my_board = board_list.get(board_identifier);
			try {
				queue.put(my_board);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		/* now switch ON the flag 
		 * to inform threads that master
		 * thread has finished filling up threads
		 */
		queue_filled_flag = true;

	}//generate_board end here

	private class thread_routine extends Thread {

		public void run()
		{
			while(true)
			{
				Board board = null;
				int result = 0;

				/* firstly check whether master thread has finished filling up queue
				 * if yes, then Is the queue empty()? if yes, then break out.
				 * This only works on the assumption that master thread will always
				 * finish faster than threads consuming boards
				 */
				if(IdaNode.queue_filled_flag)
				{
					if(IdaNode.queue.isEmpty())
						break;
				}
				/* try to take() a board from the queue 
				 * as soon as master thread starts filling
				 * the shared queue, threads will activate
				 */
				try {
					board = (Board) IdaNode.queue.take();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.out.println("Thread successfully picked a board");//DEBUG
				if(IdaNode.cache_flag)
				{
					String astring = currentThread().getName();
					int aint = Integer.parseInt(astring);
					result = Ida.solutions(board, thread_cache[aint]);
					IdaNode.addSolutions(result);
				}
				else
				{
					result = Ida.solutions(board);
					IdaNode.addSolutions(result);
				}
			}//while block ends here
		}//run block ends here
	}//thread_routine ends here
}//IdaNode class ends here
