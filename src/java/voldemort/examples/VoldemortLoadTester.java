package voldemort.examples;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.versioning.Versioned;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class VoldemortLoadTester implements Runnable {

    Logger logger = LoggerFactory.getLogger(VoldemortLoadTester.class);

    ExecutorService executorService;
    StoreClient<String, String> client;
    String bootstrapUrl;

    final int newFixedThreadPool = 100;
    final int numberOfPuts = 100000;
    final int numberOfGets = 1000;


    public static void main(String[] args) {

        String url = "tcp://127.0.0.1:6666";
        if(args.length > 1) {
            url = args[1];
        }

        VoldemortLoadTester loadGeneratorExample = new VoldemortLoadTester(url);

        Thread t = new Thread(loadGeneratorExample);
        t.start();

    }

    public VoldemortLoadTester(String url) {
        logger.info("Staring on url: {}", url);

        this.bootstrapUrl = url;

        StoreClientFactory factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootstrapUrl));
        this.client = factory.getStoreClient("test");

        this.executorService = Executors.newFixedThreadPool(newFixedThreadPool);

    }

    @Override
    public void run() {

        long startTime = System.currentTimeMillis();

        List<Future> futures = Lists.newLinkedList();
        for (int i = 0; i < numberOfPuts; i++) {
            Future future = executorService.submit(new PutJob(client, "knut" + String.valueOf(i), "toto" + String.valueOf(i)));
            futures.add(future);
        }

//        for (int i = 0; i < numberOfGets; i++) {
//            version = client.get("knut" + i);
//        }

        // dont take any more new tasks
        executorService.shutdown();
        try {
            for (Future f : futures) {
                f.get();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        long stopTime = System.currentTimeMillis();
        System.out.println("Time to put " + numberOfPuts + " values: " + (stopTime - startTime)
                + "ms");

    }

    class PutJob implements Runnable {
        Logger logger = LoggerFactory.getLogger(PutJob.class);

        StoreClient<String, String> client;
        String key;
        String value;

        public PutJob(StoreClient<String, String> client, String key, String value) {
            this.key = key;
            this.value = value;
            this.client = client;
        }

        @Override
        public void run() {
            Versioned<String> version = new Versioned<String>(bootstrapUrl);
            Versioned<String> returnVersion;

            boolean success = false;

            Long before = System.currentTimeMillis();
            returnVersion = client.get(key);
            if (returnVersion == null) {
                version.setObject(value);
                client.put(key, version);
                success = true;
            } else {
                returnVersion.setObject(value);
                client.put(key, returnVersion);
                success = true;
            }
            Long after = System.currentTimeMillis();

            Long duration = after - before;
            if(success) {
                logger.debug("Put {} {}ms", key, duration);
            } else {
                logger.debug("Put failed: {}", key);
            }
        }
    }

}
