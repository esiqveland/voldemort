package voldemort.examples;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.versioning.Version;
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


    String prefix;
    String operation;

    final int newFixedThreadPool = 200;
    final int numberOfPuts = 1000000;
    final int numberOfGets = 1000000;


    public static void main(String[] args) {

        String operation = "PUT";

        String prefix = "knut";

        String url = "tcp://127.0.0.1:6666";
        if (args.length >= 1) {
            url = args[0];
        }

        if (args.length >= 2) {
            operation = args[1];
        }

        if(args.length >= 3) {
            prefix = args[2];
        }

        VoldemortLoadTester loadGeneratorExample = new VoldemortLoadTester(url,  operation, prefix);

        Thread t = new Thread(loadGeneratorExample);
        t.start();

    }

    public VoldemortLoadTester(String url, String operation, String prefix) {
        logger.info("Staring {} on url: {}", operation, url);

        this.operation = operation;
        this.bootstrapUrl = url;
        this.prefix = prefix;

        StoreClientFactory factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootstrapUrl));
        this.client = factory.getStoreClient("test");

        this.executorService = Executors.newFixedThreadPool(newFixedThreadPool);

    }

    @Override
    public void run() {

        long startTime = System.currentTimeMillis();

        List<Future> futures = Lists.newLinkedList();

        switch (operation) {
            case "GET":
                for (int i = 0; i < numberOfGets; i++) {
                    Future future = executorService.submit(new GetJob(client, prefix + String.valueOf(i), "toto" + String.valueOf(i)));
                    futures.add(future);
                }
                break;
            case "PUT":
                for (int i = 0; i < numberOfPuts; i++) {
                    Future future = executorService.submit(new PutJob(client, prefix + String.valueOf(i), "toto" + String.valueOf(i)));
                    futures.add(future);
                }
                break;
            default:
                logger.error("Unsupported operation: {}", operation);

        }

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

    class GetJob implements Runnable {
        Logger logger = LoggerFactory.getLogger(GetJob.class);

        StoreClient<String, String> client;
        String key;
        String value;

        public GetJob(StoreClient<String, String> client, String key, String value) {
            this.key = key;
            this.value = value;
            this.client = client;
        }

        @Override
        public void run() {
            Long before = System.currentTimeMillis();

            Versioned<String> returnVersion;
            try {

                returnVersion = client.get(key);

                Long after = System.currentTimeMillis();
                logger.debug("Get {} {}ms", key, after - before);

            } catch (Exception e) {
                logger.error("Put failed: key: {} e: {}", key, e);
            }
        }

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
            try {

//                returnVersion = client.get(key);
//                if (returnVersion == null) {
//                    version.setObject(value);
//                } else {
//                    returnVersion.setObject(value);
//                }
//                client.put(key, version);

                client.put(key, value);
                success = true;
            } catch (Exception e) {
                logger.error("Put failed: key: {} e: {}", key, e);
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
