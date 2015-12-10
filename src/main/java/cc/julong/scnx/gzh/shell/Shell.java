package cc.julong.scnx.gzh.shell;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * shell脚本执行工具
 *
 * Created by zhangfeng on 2015/4/17.
 */
public abstract class Shell {

    public static class ShellCommandExecutor extends Shell {

        private final String[] command;
        private StringBuffer output;

        public ShellCommandExecutor(String[] execString) {
            command = execString.clone();
        }

        public ShellCommandExecutor(String[] execString, File dir) {
            this(execString);
            this.setWorkingDirectory(dir);
        }

        public ShellCommandExecutor(String[] execString, File dir, Map<String, String> env) {
            this(execString, dir);
            this.setEnvironment(env);
        }

        public void execute() throws IOException {
            this.run();
        }

        public String getOutput() {
            return (output == null) ? "" : output.toString();
        }

        @Override
        protected String[] getExecString() {
            return command;
        }

        @Override
        protected void parseExecResult(BufferedReader lines) throws IOException {
            output = new StringBuffer();
            char[] buf = new char[512];
            int nRead;
            while ((nRead = lines.read(buf, 0, buf.length)) > 0) {
                output.append(buf, 0, nRead);
            }
        }
    }

    static class ExitCodeException extends IOException {

        private static final long serialVersionUID = -7102633161419484030L;

        int exitCode;

        public ExitCodeException(int exitCode, String message) {
            super(message);
            this.exitCode = exitCode;
        }

        public int getExitCode() {
            return exitCode;
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(Shell.class);
    public static final boolean WINDOWS = System.getProperty("os.name").startsWith("Windows");

    public static String execCommand(Map<String, String> env, String... cmd) throws IOException {
        ShellCommandExecutor exec = new ShellCommandExecutor(cmd);
        if (env != null) {
            exec.setEnvironment(env);
        }
        exec.execute();
        return exec.getOutput();
    }

    public static String execCommand(String... cmd) throws IOException {
        return execCommand(null, cmd);
    }

    private Map<String, String> environment;

    private File dir;

    private Process process;

    private int exitCode;

    public int getExitCode() {
        return exitCode;
    }

    public Process getProcess() {
        return process;
    }

    protected abstract String[]  getExecString();

    protected abstract void parseExecResult(BufferedReader lines) throws IOException;

    protected void run() throws IOException {
        exitCode = 0; //reset
        runCommand();
    }

    protected void setEnvironment(Map<String, String> env) {
        this.environment = env;
    }

    protected void setWorkingDirectory(File dir) {
        this.dir = dir;
    }

    private void runCommand() throws IOException {
        ProcessBuilder builder = new ProcessBuilder(getExecString());
        boolean completed = false;

        if (environment != null) {
            builder.environment().putAll(this.environment);
        }
        if (dir != null) {
            builder.directory(this.dir);
        }

        process = builder.start();
        final BufferedReader errReader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
        BufferedReader inReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        final StringBuffer errMsg = new StringBuffer();

        Thread errThread = new Thread() {
            @Override
            public void run() {
                try {
                    String line = errReader.readLine();
                    while ((line != null) && !isInterrupted()) {
                        errMsg.append(line);
                        errMsg.append(System.getProperty("line.separator"));
                        line = errReader.readLine();
                    }
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                    logger.warn("Error reading the error stream", ioe);
                }
            }
        };
        try {
            errThread.start();
        } catch (IllegalStateException ise) {
            ise.printStackTrace();
        }
        try {
            parseExecResult(inReader);
            exitCode = process.waitFor();
            try {
                errThread.join();
            } catch (InterruptedException ie) {
                ie.printStackTrace();
                logger.warn("Interrupted while reading the error stream", ie);
            }
            completed = true;
            if (exitCode != 0) {
                throw new ExitCodeException(exitCode, errMsg.toString());
            }
        } catch (InterruptedException ie) {
            throw new IOException(ie.toString());
        } finally {
            try {
                inReader.close();
            } catch (IOException ioe) {
                logger.warn("Error while closing the input stream", ioe);
            }
            if (!completed) {
                errThread.interrupt();
            }
            try {
                errReader.close();
            } catch (IOException ioe) {
                logger.warn("Error while closing the error stream", ioe);
            }
            process.destroy();
        }
    }
}