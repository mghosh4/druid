#terminator thread

class TerminatorThread extends Thread:
	list threads
	long maxExecutionTime
	Workload workload
	long waitTimeOutInMS

	def TerminatorThread(long maxExecutionTime, list threads, Workload workload):
		this.maxExecutionTime = maxExecutionTime
		this.threads = threads
		this.workload = workload

		waitTimeOutInMS = 2000;
		print("Maximum execution time specified as: " + maxExecutionTime + " secs")

	def run():
		try:
			#forces thread to sleep?
			time.sleep(maxExecutionTime * 1000)
		except RuntimeError:
			print("Could not wait until max specified time, TerminatorThread interrupted.")
			return
		print("Stop requested for workload. Now Joining")

		workload.requestStop()

		print("Stop requested for workload. Now Joining")

		for(Thread t : threads):
			while(t.isAlive()):
				try:
					t.join(waitTimeOutInMS)
					if(t.isAlive()):
						print("Still waiting for thread " + t.getName() + " to complete. " + "Workload status: " + workload.isStopRequested())

				except RuntimeError:
					#idk