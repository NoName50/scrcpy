package com.genymobile.scrcpy;

import com.genymobile.scrcpy.util.Ln;
import com.genymobile.scrcpy.util.LogUtils;

import android.net.LocalSocket;
import android.net.LocalSocketAddress;
import android.os.Build;
import android.os.Looper;

import com.genymobile.scrcpy.device.DesktopConnection;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public final class DaemonServer {

    private static final int DEFAULT_DAEMON_PORT = 27183;
    // Retry connecting to DesktopConnection's LocalServerSocket (it may not be ready yet)
    private static final int LOCAL_CONNECT_RETRY_MS = 100;
    private static final int LOCAL_CONNECT_MAX_RETRIES = 50; // 5 seconds total

    private static volatile boolean running;
    private static volatile ServerSocket serverSocket;
    private static int daemonPort;

    private DaemonServer() {
        // not instantiable
    }

    public static void main(String... args) {
        Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
            Ln.e("Exception on thread " + t, e);
        });

        Server.dropRootPrivileges();

        // Parse --daemon-port from args only
        daemonPort = DEFAULT_DAEMON_PORT;
        for (String arg : args) {
            if (arg.startsWith("--daemon-port=")) {
                try {
                    daemonPort = Integer.parseInt(arg.substring("--daemon-port=".length()));
                } catch (NumberFormatException e) {
                    Ln.e("Invalid daemon-port: " + arg);
                    System.exit(1);
                }
                break;
            }
        }

        Ln.i("Device: [" + Build.MANUFACTURER + "] " + Build.BRAND + " " + Build.MODEL + " (Android " + Build.VERSION.RELEASE + ")");

        try {
            start();
        } catch (Throwable t) {
            Ln.e("Daemon fatal error", t);
            System.exit(1);
        }
    }

    public static void start() throws IOException {
        int port = daemonPort;

        serverSocket = new ServerSocket(port);
        Ln.i("Daemon listening on TCP:" + port);
        running = true;

        try {
            while (running) {
                // Accept one session at a time. The client connects first
                // to send options, then connects again for each data channel.
                // All connections come through the same port.
                Socket optSocket = null;
                try {
                    optSocket = serverSocket.accept();
                } catch (IOException e) {
                    if (running) {
                        Ln.e("Daemon accept error", e);
                    }
                    break;
                }

                Ln.i("Daemon: new client connected from " + optSocket.getInetAddress());
                handleSession(optSocket);
            }
        } finally {
            closeQuietly(serverSocket);
        }

        // daemonPort < 0 means kill (exit), otherwise restart
        if (daemonPort >= 0) {
            Ln.i("Restarting daemon on port: " + daemonPort);
            start();
        }
    }

    /**
     * Handle one session. All connections (options + data) come through the
     * same serverSocket via sequential accept() calls.
     */
    private static void handleSession(Socket optSocket) {
        try {
            // Step 1: Read options from first connection
            Options options = readOptions(optSocket);
            optSocket.close();
            if (options == null) {
                return;
            }

            Ln.disableSystemStreams();
            Ln.initLogLevel(options.getLogLevel());

            // Handle kill_daemon
            if (options.getKillDaemon()) {
                Ln.i("kill_daemon requested, shutting down daemon...");
                daemonPort = -1;
                shutdownDaemon();
                return;
            }

            // Handle restart_daemon
            int restartPort = options.getRestartDaemon();
            if (restartPort > 0) {
                Ln.i("restart_daemon requested on port: " + restartPort);
                daemonPort = restartPort;
                shutdownDaemon();
                return;
            } else if (restartPort == 0) {
                Ln.i("restart_daemon requested on current port: " + daemonPort);
                shutdownDaemon();
                return;
            }

            // Handle list-only requests
            if (options.getList()) {
                if (options.getCleanup()) {
                    CleanUp.unlinkSelf();
                }
                Ln.i(LogUtils.buildVideoEncoderListMessage());
                Ln.i(LogUtils.buildAudioEncoderListMessage());
                Ln.i(LogUtils.buildDisplayListMessage());
                return;
            }

            boolean video = options.getVideo();
            boolean audio = options.getAudio();
            boolean control = options.getControl();

            // Step 2: Start scrcpy session thread.
            // DesktopConnection.open(tunnelForward=true) creates LocalServerSockets
            // and blocks on accept() for video/audio/control.
            Thread scrcpyThread = new Thread(() -> {
                Server.prepareMainLooper();
                try {
                    Server.scrcpy(options);
                } catch (Exception e) {
                    Ln.e("Session error", e);
                } finally {
                    resetLooper();
                }
            }, "scrcpy-session");
            scrcpyThread.start();

            // Give scrcpy thread time to create LocalServerSockets
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                return;
            }

            // Step 3: Accept data connections and bridge them to LocalSockets.
            // Order: video → audio → control (matches DesktopConnection.open() accept order).
            String socketName = DesktopConnection.getSocketName(options.getScid());

            List<Thread> bridgeThreads = new ArrayList<>();

            if (video) {
                Socket tcpSocket = acceptSocket();
                if (tcpSocket != null) {
                    Thread t = new Thread(() -> bridgeChannel(tcpSocket, socketName, "video"));
                    t.start();
                    bridgeThreads.add(t);
                }
            }

            if (audio) {
                Socket tcpSocket = acceptSocket();
                if (tcpSocket != null) {
                    Thread t = new Thread(() -> bridgeChannel(tcpSocket, socketName, "audio"));
                    t.start();
                    bridgeThreads.add(t);
                }
            }

            if (control) {
                Socket tcpSocket = acceptSocket();
                if (tcpSocket != null) {
                    Thread t = new Thread(() -> bridgeChannel(tcpSocket, socketName, "control"));
                    t.start();
                    bridgeThreads.add(t);
                }
            }

            // Step 4: Wait for scrcpy session to finish
            try {
                scrcpyThread.join();
            } catch (InterruptedException e) {
                // ignore
            }

            // Session ended, interrupt bridge threads
            for (Thread t : bridgeThreads) {
                t.interrupt();
            }
            for (Thread t : bridgeThreads) {
                try {
                    t.join(3000);
                } catch (InterruptedException e) {
                    break;
                }
            }
        } catch (Exception e) {
            Ln.e("Session error", e);
        }
    }

    /**
     * Accept one TCP connection from the daemon's ServerSocket.
     * Returns null if the daemon is shutting down.
     */
    private static Socket acceptSocket() {
        try {
            return serverSocket.accept();
        } catch (IOException e) {
            if (running) {
                Ln.e("Accept error", e);
            }
            return null;
        }
    }

    /**
     * Bridge one TCP socket to a LocalSocket using the given socket name.
     * This runs in its own thread and copies data bidirectionally.
     */
    private static void bridgeChannel(Socket tcpSocket, String localSocketName, String label) {
        try {
            LocalSocket localSocket = connectLocalWithRetry(localSocketName);
            Ln.i("Bridge established: " + label + " TCP <-> " + localSocketName);

            // Bidirectional copy
            Thread tcpToLocal = new Thread(() -> copyStream(tcpSocket, localSocket, label + ":TCP→Local"));
            Thread localToTcp = new Thread(() -> copyStream(localSocket, tcpSocket, label + ":Local→TCP"));
            tcpToLocal.start();
            localToTcp.start();

            try {
                tcpToLocal.join();
                localToTcp.join();
            } catch (InterruptedException e) {
                tcpToLocal.interrupt();
                localToTcp.interrupt();
            }
        } catch (IOException e) {
            Ln.e("Bridge error for " + label + ": " + e.getMessage());
        } finally {
            closeQuietly(tcpSocket);
        }
    }

    private static LocalSocket connectLocalWithRetry(String name) throws IOException {
        for (int i = 0; i < LOCAL_CONNECT_MAX_RETRIES; i++) {
            if (!running) {
                throw new IOException("Daemon stopped");
            }
            try {
                LocalSocket socket = new LocalSocket();
                socket.connect(new LocalSocketAddress(name));
                return socket;
            } catch (IOException e) {
                // LocalServerSocket not ready yet, retry
                try {
                    Thread.sleep(LOCAL_CONNECT_RETRY_MS);
                } catch (InterruptedException ie) {
                    throw new IOException("Interrupted", ie);
                }
            }
        }
        throw new IOException("Failed to connect to LocalSocket '" + name + "' after " + LOCAL_CONNECT_MAX_RETRIES + " retries");
    }

    private static void copyStream(java.io.Closeable src, java.io.Closeable dst, String label) {
        try {
            InputStream in;
            OutputStream out;

            if (src instanceof Socket) {
                in = ((Socket) src).getInputStream();
            } else if (src instanceof LocalSocket) {
                in = ((LocalSocket) src).getInputStream();
            } else {
                return;
            }

            if (dst instanceof Socket) {
                out = ((Socket) dst).getOutputStream();
            } else if (dst instanceof LocalSocket) {
                out = ((LocalSocket) dst).getOutputStream();
            } else {
                return;
            }

            byte[] buf = new byte[8192];
            int n;
            while ((n = in.read(buf)) != -1) {
                out.write(buf, 0, n);
                out.flush();
            }
        } catch (IOException e) {
            // Session ended, expected
        } finally {
            closeQuietly(src);
            closeQuietly(dst);
        }
    }

    private static void shutdownDaemon() {
        running = false;
        if (serverSocket != null && !serverSocket.isClosed()) {
            try {
                serverSocket.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }

    private static Options readOptions(Socket socket) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        List<String> argsList = new ArrayList<>();
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.isEmpty()) {
                break; // Empty line marks end of options
            }
            argsList.add(line);
        }
        if (argsList.isEmpty()) {
            return null;
        }
        // Daemon mode always uses tunnel_forward: the bridge thread bridges
        // TCP connections to LocalSocket, so DesktopConnection must act as
        // a LocalServerSocket acceptor, not a LocalSocket client.
        argsList.add("tunnel_forward=true");
        String[] argsArray = argsList.toArray(new String[0]);
        return Options.parse(argsArray);
    }

    private static void resetLooper() {
        try {
            Field field = Looper.class.getDeclaredField("sMainLooper");
            field.setAccessible(true);
            field.set(null, null);
        } catch (Exception e) {
            Ln.e("Failed to reset sMainLooper", e);
        }
        try {
            Field threadLocalField = Looper.class.getDeclaredField("sThreadLocal");
            threadLocalField.setAccessible(true);
            ThreadLocal<?> threadLocal = (ThreadLocal<?>) threadLocalField.get(null);
            threadLocal.remove();
        } catch (Exception e) {
            Ln.e("Failed to reset sThreadLocal", e);
        }
    }

    private static void closeQuietly(java.io.Closeable c) {
        if (c != null) {
            try {
                c.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }
}