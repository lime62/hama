/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hama.bsp;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.bsp.BSPMaster.State;
import org.apache.hama.ipc.JobSubmissionProtocol;
import org.apache.zookeeper.KeeperException;

/**
 * A multithreaded local BSP runner that can be used for debugging BSP's. It
 * uses the working directory "/user/hama/bsp/" and starts runners based on the
 * number of the machines core.
 * 
 */
public class LocalBSPRunner implements JobSubmissionProtocol {

  private static final String IDENTIFIER = "localrunner";
  private static String WORKING_DIR = "/user/hama/bsp/";
  protected static volatile ThreadPoolExecutor threadPool;
  protected static final int threadPoolSize;
  protected static final LinkedList<Future<BSP>> futureList = new LinkedList<Future<BSP>>();
  protected static CyclicBarrier barrier;

  static {
    threadPoolSize = Runtime.getRuntime().availableProcessors();
    barrier = new CyclicBarrier(threadPoolSize);
    threadPool = (ThreadPoolExecutor) Executors
        .newFixedThreadPool(threadPoolSize);
  }

  protected HashMap<String, BSPPeerProtocol> localGrooms = new HashMap<String, BSPPeerProtocol>();
  protected String jobFile;
  protected String jobName;

  protected JobStatus currentJobStatus;

  protected Configuration conf;
  protected FileSystem fs;

  {
    for (int i = 0; i < threadPoolSize; i++) {
      String name = IDENTIFIER + " " + i;
      localGrooms.put(name, new LocalGroom(name));
    }
  }

  public LocalBSPRunner(Configuration conf) throws IOException {
    super();
    this.conf = conf;
    this.fs = FileSystem.get(conf);
    String path = conf.get("bsp.local.dir");
    if (path != null && !path.isEmpty())
      WORKING_DIR = path;
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    return 3;
  }

  @Override
  public BSPJobID getNewJobId() throws IOException {
    return new BSPJobID(IDENTIFIER, 1);
  }

  @Override
  public JobStatus submitJob(BSPJobID jobID, String jobFile) throws IOException {
    this.jobFile = jobFile;
    BSPJob job = new BSPJob(jobID, jobFile);
    job.setNumBspTask(threadPoolSize);
    this.jobName = job.getJobName();
    currentJobStatus = new JobStatus(jobID, System.getProperty("user.name"), 0,
        JobStatus.RUNNING);
    for (int i = 0; i < threadPoolSize; i++) {
      String name = IDENTIFIER + " " + i;
      BSPPeerProtocol localGroom = new LocalGroom(name);
      localGrooms.put(name, localGroom);
      futureList.add(threadPool.submit(new BSPRunner(conf, job, ReflectionUtils
          .newInstance(job.getBspClass(), conf), localGroom)));
    }
    new Thread(new ThreadObserver(currentJobStatus)).start();
    return currentJobStatus;
  }

  @Override
  public ClusterStatus getClusterStatus(boolean detailed) throws IOException {
    Map<String, String> map = new HashMap<String, String>();
    for (Entry<String, BSPPeerProtocol> entry : localGrooms.entrySet()) {
      map.put(entry.getKey(), entry.getValue().getPeerName());
    }
    return new ClusterStatus(map, 0, 1, State.RUNNING);
  }

  @Override
  public JobProfile getJobProfile(BSPJobID jobid) throws IOException {
    return new JobProfile(System.getProperty("user.name"), jobid, jobFile,
        jobName);
  }

  @Override
  public JobStatus getJobStatus(BSPJobID jobid) throws IOException {
    if (currentJobStatus == null) {
      currentJobStatus = new JobStatus(jobid, System.getProperty("user.name"),
          0L, JobStatus.RUNNING);
    }
    return currentJobStatus;
  }

  @Override
  public String getFilesystemName() throws IOException {
    return fs.getUri().toString();
  }

  @Override
  public JobStatus[] jobsToComplete() throws IOException {
    return null;
  }

  @Override
  public JobStatus[] getAllJobs() throws IOException {
    return null;
  }

  @Override
  public String getSystemDir() {
    return WORKING_DIR;
  }

  @Override
  public void killJob(BSPJobID jobid) throws IOException {
    return;
  }

  @Override
  public boolean killTask(TaskAttemptID taskId, boolean shouldFail)
      throws IOException {
    return false;
  }

  // this class will spawn a new thread and executes the BSP
  class BSPRunner implements Callable<BSP> {

    Configuration conf;
    BSPJob job;
    BSP bsp;
    BSPPeerProtocol groom;

    public BSPRunner(Configuration conf, BSPJob job, BSP bsp,
        BSPPeerProtocol groom) {
      super();
      this.conf = conf;
      this.job = job;
      this.bsp = bsp;
      this.groom = groom;
    }

    public void run() {
      bsp.setConf(conf);
      try {
        bsp.bsp(groom);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    @Override
    public BSP call() throws Exception {
      run();
      return bsp;
    }
  }

  // this thread observes the status of the runners.
  class ThreadObserver implements Runnable {

    JobStatus status;

    public ThreadObserver(JobStatus currentJobStatus) {
      this.status = currentJobStatus;
    }

    @Override
    public void run() {
      boolean success = true;
      for (Future<BSP> future : futureList) {
        try {
          future.get();
        } catch (InterruptedException e) {
          e.printStackTrace();
          success = false;
        } catch (ExecutionException e) {
          e.printStackTrace();
          success = false;
        }
      }
      if (success) {
        currentJobStatus.setState(JobStatus.State.SUCCEEDED);
        currentJobStatus.setRunState(JobStatus.SUCCEEDED);
      } else {
        currentJobStatus.setState(JobStatus.State.FAILED);
        currentJobStatus.setRunState(JobStatus.FAILED);
      }
      threadPool.shutdownNow();
    }

  }

  class LocalGroom implements BSPPeerProtocol {
    private static final String FIRST_THREAD_NAME = "pool-1-thread-1";
    private long superStepCount = 0;
    private final ConcurrentLinkedQueue<BSPMessage> localMessageQueue = new ConcurrentLinkedQueue<BSPMessage>();
    // outgoing queue
    private final Map<String, ConcurrentLinkedQueue<BSPMessage>> outgoingQueues = new ConcurrentHashMap<String, ConcurrentLinkedQueue<BSPMessage>>();
    private final String peerName;

    public LocalGroom(String peerName) {
      super();
      this.peerName = peerName;
    }

    @Override
    public void send(String peerName, BSPMessage msg) throws IOException {
      if (this.peerName.equals(peerName)) {
        put(msg);
      } else {
        // put this into a outgoing queue
        if (outgoingQueues.get(peerName) == null) {
          outgoingQueues.put(peerName, new ConcurrentLinkedQueue<BSPMessage>());
        }
        outgoingQueues.get(peerName).add(msg);
      }
    }

    @Override
    public void put(BSPMessage msg) throws IOException {
      localMessageQueue.add(msg);
    }

    @Override
    public BSPMessage getCurrentMessage() throws IOException {
      return localMessageQueue.poll();
    }

    @Override
    public int getNumCurrentMessages() {
      return localMessageQueue.size();
    }

    @Override
    public void sync() throws IOException, KeeperException,
        InterruptedException {
      // wait until all threads reach this barrier
      barrierSync();
      // send the messages
      for (Entry<String, ConcurrentLinkedQueue<BSPMessage>> entry : outgoingQueues
          .entrySet()) {
        String peerName = entry.getKey();
        for (BSPMessage msg : entry.getValue())
          localGrooms.get(peerName).put(msg);
      }
      // clear the local outgoing queue
      outgoingQueues.clear();
      // sync again to avoid data inconsistency
      barrierSync();
      incrementSuperSteps();
    }

    private void barrierSync() throws InterruptedException {
      try {
        barrier.await();
      } catch (BrokenBarrierException e) {
        throw new InterruptedException("Barrier has been broken!" + e);
      }
    }

    private void incrementSuperSteps() {
      // just let the first thread set the supersteps
      if (Thread.currentThread().getName().equals(FIRST_THREAD_NAME)) {
        currentJobStatus.setprogress(superStepCount++);
        currentJobStatus.setSuperstepCount(currentJobStatus.progress());
      }
    }

    @Override
    public long getSuperstepCount() {
      return superStepCount;
    }

    @Override
    public String getPeerName() {
      return peerName;
    }

    @Override
    public String[] getAllPeerNames() {
      return localGrooms.keySet().toArray(
          new String[localGrooms.keySet().size()]);
    }

    @Override
    public void clear() {
      localMessageQueue.clear();
    }

    @Override
    public long getProtocolVersion(String protocol, long clientVersion)
        throws IOException {
      return 3;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public Task getTask(TaskAttemptID taskid) throws IOException {
      return null;
    }

    @Override
    public boolean ping(TaskAttemptID taskid) throws IOException {
      return true;
    }

    @Override
    public void done(TaskAttemptID taskid, boolean shouldBePromoted)
        throws IOException {

    }

    @Override
    public void fsError(TaskAttemptID taskId, String message)
        throws IOException {

    }

    @Override
    public void put(BSPMessageBundle messages) throws IOException {
      throw new UnsupportedOperationException(
          "Messagebundle is not supported by local testing");
    }

  }

}