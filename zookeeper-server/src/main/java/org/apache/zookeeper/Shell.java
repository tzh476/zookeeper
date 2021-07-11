package org.apache.zookeeper;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.zookeeper.common.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


abstract public class Shell {
  
    private static final Logger LOG = LoggerFactory.getLogger(Shell.class);
  
  
  public final static String USER_NAME_COMMAND = "whoami";
  
  public static String[] getGroupsCommand() {
    return new String[]{"bash", "-c", "groups"};
  }
  
  public static String[] getGroupsForUserCommand(final String user) {
        return new String [] {"bash", "-c", "id -Gn " + user};
  }
  
  public static final String SET_PERMISSION_COMMAND = "chmod";
  
  public static final String SET_OWNER_COMMAND = "chown";
  public static final String SET_GROUP_COMMAND = "chgrp";
  
  public static String[] getGET_PERMISSION_COMMAND() {
        return new String[] {(WINDOWS ? "ls" : "/bin/ls"), "-ld"};
  }

  
  protected long timeOutInterval = 0L;
  
  private AtomicBoolean timedOut;

  
  public static final String ULIMIT_COMMAND = "ulimit";
  
  
  public static String[] getUlimitMemoryCommand(int memoryLimit) {
        if (WINDOWS) {
      return null;
    }
    
    return new String[] {ULIMIT_COMMAND, "-v", String.valueOf(memoryLimit)};
  }

  
  public static final boolean WINDOWS 
                = System.getProperty("os.name").startsWith("Windows");
  
  private long    interval;     private long    lastTime;     private Map<String, String> environment;   private File dir;
  private Process process;   private int exitCode;

  
  private volatile AtomicBoolean completed;
  
  public Shell() {
    this(0L);
  }
  
  
  public Shell( long interval ) {
    this.interval = interval;
    this.lastTime = (interval<0) ? 0 : -interval;
  }
  
  
  protected void setEnvironment(Map<String, String> env) {
    this.environment = env;
  }

  
  protected void setWorkingDirectory(File dir) {
    this.dir = dir;
  }

  
  protected void run() throws IOException {
    if (lastTime + interval > Time.currentElapsedTime())
      return;
    exitCode = 0;     runCommand();
  }

  
  private void runCommand() throws IOException { 
    ProcessBuilder builder = new ProcessBuilder(getExecString());
    Timer timeOutTimer = null;
    ShellTimeoutTimerTask timeoutTimerTask = null;
    timedOut = new AtomicBoolean(false);
    completed = new AtomicBoolean(false);
    
    if (environment != null) {
      builder.environment().putAll(this.environment);
    }
    if (dir != null) {
      builder.directory(this.dir);
    }
    
    process = builder.start();
    if (timeOutInterval > 0) {
      timeOutTimer = new Timer();
      timeoutTimerTask = new ShellTimeoutTimerTask(
          this);
            timeOutTimer.schedule(timeoutTimerTask, timeOutInterval);
    }
    final BufferedReader errReader = 
            new BufferedReader(new InputStreamReader(process
                                                     .getErrorStream()));
    BufferedReader inReader = 
            new BufferedReader(new InputStreamReader(process
                                                     .getInputStream()));
    final StringBuffer errMsg = new StringBuffer();
    
            Thread errThread = new Thread() {
      @Override
      public void run() {
        try {
          String line = errReader.readLine();
          while((line != null) && !isInterrupted()) {
            errMsg.append(line);
            errMsg.append(System.getProperty("line.separator"));
            line = errReader.readLine();
          }
        } catch(IOException ioe) {
          LOG.warn("Error reading the error stream", ioe);
        }
      }
    };
    try {
      errThread.start();
    } catch (IllegalStateException ise) { }
    try {
      parseExecResult(inReader);             String line = inReader.readLine();
      while(line != null) { 
        line = inReader.readLine();
      }
            exitCode  = process.waitFor();
      try {
                errThread.join();
      } catch (InterruptedException ie) {
        LOG.warn("Interrupted while reading the error stream", ie);
      }
      completed.set(true);
                  if (exitCode != 0) {
        throw new ExitCodeException(exitCode, errMsg.toString());
      }
    } catch (InterruptedException ie) {
      throw new IOException(ie.toString());
    } finally {
      if ((timeOutTimer!=null) && !timedOut.get()) {
        timeOutTimer.cancel();
      }
            try {
        inReader.close();
      } catch (IOException ioe) {
        LOG.warn("Error while closing the input stream", ioe);
      }
      if (!completed.get()) {
        errThread.interrupt();
      }
      try {
        errReader.close();
      } catch (IOException ioe) {
        LOG.warn("Error while closing the error stream", ioe);
      }
      process.destroy();
      lastTime = Time.currentElapsedTime();
    }
  }

   
  protected abstract String[] getExecString();
  
  
  protected abstract void parseExecResult(BufferedReader lines)
  throws IOException;

  
  public Process getProcess() {
    return process;
  }

  
  public int getExitCode() {
    return exitCode;
  }

  
  @SuppressWarnings("serial")
  public static class ExitCodeException extends IOException {
    int exitCode;
    
    public ExitCodeException(int exitCode, String message) {
      super(message);
      this.exitCode = exitCode;
    }
    
    public int getExitCode() {
      return exitCode;
    }
  }
  
  
  public static class ShellCommandExecutor extends Shell {
    
    private String[] command;
    private StringBuffer output;
    
    
    public ShellCommandExecutor(String[] execString) {
      this(execString, null);
    }
    
    public ShellCommandExecutor(String[] execString, File dir) {
      this(execString, dir, null);
    }
   
    public ShellCommandExecutor(String[] execString, File dir, 
                                 Map<String, String> env) {
      this(execString, dir, env , 0L);
    }

    
    public ShellCommandExecutor(String[] execString, File dir, 
        Map<String, String> env, long timeout) {
      command = execString.clone();
      if (dir != null) {
        setWorkingDirectory(dir);
      }
      if (env != null) {
        setEnvironment(env);
      }
      timeOutInterval = timeout;
    }
        

    
    public void execute() throws IOException {
      this.run();    
    }

    protected String[] getExecString() {
      return command;
    }

    protected void parseExecResult(BufferedReader lines) throws IOException {
      output = new StringBuffer();
      char[] buf = new char[512];
      int nRead;
      while ( (nRead = lines.read(buf, 0, buf.length)) > 0 ) {
        output.append(buf, 0, nRead);
      }
    }
    
    
    public String getOutput() {
      return (output == null) ? "" : output.toString();
    }

    
    public String toString() {
      StringBuilder builder = new StringBuilder();
      String[] args = getExecString();
      for (String s : args) {
        if (s.indexOf(' ') >= 0) {
          builder.append('"').append(s).append('"');
        } else {
          builder.append(s);
        }
        builder.append(' ');
      }
      return builder.toString();
    }
  }
  
  
  public boolean isTimedOut() {
    return timedOut.get();
  }
  
  
  private void setTimedOut() {
    this.timedOut.set(true);
  }
  
  
  public static String execCommand(String ... cmd) throws IOException {
    return execCommand(null, cmd, 0L);
  }
  
  
  
  public static String execCommand(Map<String, String> env, String[] cmd,
      long timeout) throws IOException {
    ShellCommandExecutor exec = new ShellCommandExecutor(cmd, null, env, 
                                                          timeout);
    exec.execute();
    return exec.getOutput();
  }

  
  public static String execCommand(Map<String,String> env, String ... cmd) 
  throws IOException {
    return execCommand(env, cmd, 0L);
  }
  
  
  private static class ShellTimeoutTimerTask extends TimerTask {

    private Shell shell;

    public ShellTimeoutTimerTask(Shell shell) {
      this.shell = shell;
    }

    @Override
    public void run() {
      Process p = shell.getProcess();
      try {
        p.exitValue();
      } catch (Exception e) {
                                if (p != null && !shell.completed.get()) {
          shell.setTimedOut();
          p.destroy();
        }
      }
    }
  }
}
