package voldemort.headmaster.sigar;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import voldemort.headmaster.Headmaster;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;

public class SigarReceiver implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(SigarReceiver.class);

    private int listenPort;
    private DatagramSocket socket;
    private boolean running = false;
    private StatusMessageListener sigarMessageListener;

    public SigarReceiver(int listenPort, StatusMessageListener statusMessageListener) {
        this.listenPort = listenPort;
        this.sigarMessageListener = statusMessageListener;
        setupSocket();
    }

    private void setupSocket() {
        try {
            socket = new DatagramSocket(this.listenPort);
            setRunning(true);

            logger.debug("running ok");
        } catch (SocketException e) {
            logger.error("Error setting up socket on port: {}", this.listenPort, e);
        }
    }


    @Override
    public void run() {
        byte[] data = new byte[1024];

        logger.info("Starting SigarReceiver on port: {}", listenPort);
        while (isRunning()) {
            DatagramPacket packet = new DatagramPacket(data, data.length);
            try {
                socket.receive(packet);
                byte[] recv = packet.getData();
                ByteArrayInputStream in = new ByteArrayInputStream(recv);
                ObjectInputStream is = new ObjectInputStream(in);
                try {
                    SigarStatusMessage message = (SigarStatusMessage) is.readObject();

                    notifyListeners(message);

                } catch (ClassCastException | ClassNotFoundException e) {
                    logger.error("error converting message data from {}", packet.getAddress(), e);
                }

            } catch (IOException e) {
                logger.error("Error receiving packet", e);
                setRunning(false);
                if(socket.isClosed()) {
                    setupSocket();
                    setRunning(true);
                }
            } catch(Exception e) {
                logger.error("error in receiver for status messages",e);
            }
        }
    }

    private void notifyListeners(SigarStatusMessage message) {
//        logger.debug("status: {}", message);
        if(sigarMessageListener != null) {
            sigarMessageListener.statusMessage(message);
        }
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    public boolean isRunning() {
        return running;
    }

    public static void main(String[] args) {
        SigarReceiver sigarReceiver = new SigarReceiver(Headmaster.HEADMASTER_SIGAR_LISTENER_PORT, null);

        Thread t = new Thread(sigarReceiver);
        t.start();

    }
}
