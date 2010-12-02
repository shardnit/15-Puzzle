package ida.ipl;

import java.io.IOException;
import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
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
	private PortType id_port =
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
		ibis = IbisFactory.createIbis(ibisCapabilities, null, broadcast_port, solution_port, id_port);
	}

	public void run(int thr) throws Exception {
		registry = ibis.registry();

		/* initialise the threads */
		threads = new thread_routine[total_threads];
		/*elect a server*/
		IbisIdentifier server = registry.elect("master");

		if (server.equals(ibis.identifier())) {
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
		ReceivePort receive_solution_port;

		/* send port on master:
		 * - to send tasks(boards) to workers
		 * - to send information messages to workers (and itself)
		 */
		SendPort send_id_port;
		//port used by master to send information_messages to client
		SendPort send_info_port;


		IbisIdentifier[] joinedIbises;
		long time_taken;

		public master() throws IOException {
			solutions = 0;
			bound = board_initial.distance();

			receive_solution_port = ibis.createReceivePort(solution_port, "solution");

			//enable connection
			receive_solution_port.enableConnections();

			/*send port of master*/
			send_info_port = ibis.createSendPort(broadcast_port);
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

			System.out.println("Running IDA*, initial board:");
			System.out.println(board_initial);

			/* Now connect the information broadcast
			 * port to all the machines(including) master
			 * in ibis pool
			 */
			for(int i=0; i<total_machines; i++)
			{
				if (!(joinedIbises[i].equals(ibis.identifier())))
				{
					send_info_port.connect(joinedIbises[i], "info");
				}
			}

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
					send_id_port = ibis.createSendPort(id_port);
					send_id_port.connect(joinedIbises[i], "id");
					WriteMessage m = send_id_port.newMessage();
					m.writeInt(assigned_ids);
					m.finish();
					send_id_port.disconnect(joinedIbises[i], "id");
					send_id_port.close();
					assigned_ids++;
				}
			}
			
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
					WriteMessage m1 = send_info_port.newMessage();
					m1.writeInt(-1);
					m1.finish();
					break;
				}

				else
				{
					/*give the signal to worker to CONTINUE*/
					WriteMessage m = send_info_port.newMessage();
					m.writeInt(0);
					m.finish();
				}

				System.out.print(bound + " ");
				System.out.flush();

				/*Initialising the job queue with:
				 * capacity = capacity of board cache = 10*1024
				 * FIFO = ON 
				 */
				queue = new ArrayBlockingQueue<Object>(10*1024, true);


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

				/* checking whether the queue has something or not 
				 * if not, move to the next bound
				 */
				if(queue.size()==0)
				{
					bound +=2;
					continue;
				}

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

				/* wait for all other nodes to report back the results */
				for(int i=1; i<total_machines; i++)
				{
					ReadMessage worker_solutions = receive_solution_port.receive();
					int child_sol = worker_solutions.readInt();
					worker_solutions.finish();
					addSolutions(child_sol);
				}
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
		ReceivePort receive_id_port, receive_info_port;
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

			receive_id_port = ibis.createReceivePort(id_port, "id");
			receive_id_port.enableConnections();

			receive_info_port = ibis.createReceivePort(broadcast_port, "info");
			receive_info_port.enableConnections();
		}
		public void run() throws IOException, InterruptedException {

			/* first receive the machineID */
			ReadMessage msg_id = receive_id_port.receive();
			if(myId==-1)
				myId = msg_id.readInt();
			msg_id.finish();

			receive_id_port.close();
			
			/* receive the total_machines from Server */
			ReadMessage msg_total_machines = receive_info_port.receive();
			IdaNode.total_machines = msg_total_machines.readInt();
			msg_total_machines.finish();
				
			bound = board_initial.distance();
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
				{
					break;
				}

				/* CONTINUE signal */
				else
				{
					/*Initialising the job queue with:
					 * capacity = capacity of board cache = 10*1024
					 * FIFO = ON 
					 */
					queue = new ArrayBlockingQueue<Object>(10*1024, true);
					
					board_initial.setBound(bound);
					//expand the board
					generate_boards(board_initial);
				
					/* checking whether the queue has something or not 
				 	* if not, move to the next bound
				 	*/
					if(queue.size()==0)
					{
						bound +=2;
						continue;
					}	
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
					
					WriteMessage msg_solution = send_solution_port.newMessage();
					msg_solution.writeInt(solutions);
					msg_solution.finish();
					bound +=2;
				}//else block end here
			}//while block ends here
			/* close all the ports */
			send_solution_port.close();
		}//run ends here

	}//worker class ends here

	public void generate_boards(Board board) {
		LinkedList<Board> board_list = new LinkedList<Board>();
		Board child_board = null;
		Board my_board = null;
		int board_identifier;
		board_list.add(board);
		/* now fill the list until we have pre-defined number of boards */
		while(board_list.size()< 4000)
		{
			if(board_list.size()==0)
				break;
			child_board = board_list.removeFirst();
			/* now perform some optimization checks */
			if(child_board.distance()==0)
			{
				addSolutions(1);
				continue;
			}
			if(child_board.distance()>child_board.bound())
				continue;

			/* 1. now expand the baord 
			 * 2. Add the boards to the linked list
			 */
			Board[] children_array = child_board.makeMoves();

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
			try {
				my_board = board_list.get(board_identifier);
				queue.put(my_board);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.exit(1);
			}
			catch (IndexOutOfBoundsException e) {
				break;
			}
		}

		/* now switch ON the flag 
		 * to inform threads that master
		 * thread has finished filling up threads
		 */
		queue_filled_flag = true;

	}//generate_board end herle

	private synchronized static boolean check_queue()
	{
		if(IdaNode.queue.isEmpty())
			return true;
		else
			return false;
	}
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
					if(IdaNode.check_queue())
					{
						return;
					}
				}
				/* try to take() a board from the queue 
				 * as soon as master thread starts filling
				 * the shared queue, threads will activate
				 */
				try {
					
					if(IdaNode.queue_filled_flag)
					{
						board = (Board) IdaNode.queue.poll(1, TimeUnit.NANOSECONDS);
						if(board==null)
						{
							if(IdaNode.queue_filled_flag)
								break;
						}
					}
					else
						board = (Board) IdaNode.queue.take();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				if(IdaNode.cache_flag)
				{
					String astring = currentThread().getName();
					int aint = Integer.parseInt(astring);
					result = Ida.solutions(board, thread_cache[aint]);
					addSolutions(result);
				}
				else
				{
					result = Ida.solutions(board);
		 			addSolutions(result);
				}
			}//while block ends here
		}//run block ends here
	}//thread_routine ends here
}//IdaNode class ends here
