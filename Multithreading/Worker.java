import java.io.File;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.Map;
import java.util.HashMap;


public class Worker implements Runnable {

    private static final int CONSUMER_COUNT = 5;
    private final static BlockingQueue<byte[]> chunkQueue = new ArrayBlockingQueue<>(30);
    private final static BlockingQueue<String> lineQueue = new ArrayBlockingQueue<>(30);
    private final static BlockingQueue<String> consumerIsDone = new ArrayBlockingQueue<>(30);

    private boolean isConsumer = false;
    private static boolean producerIsDone = false;
    private static ArrayList<String> nameList = new ArrayList<String>();
    private static float numberOfChunks = 0;

    public Worker(boolean consumer) {
        this.isConsumer = consumer;
    }

    public static void main(String[] args) throws InterruptedException {
        Date start = new Date();

        ExecutorService producerPool = Executors.newFixedThreadPool(1);
        producerPool.submit(new Worker(false)); // run method is called

        // create a pool of consumer threads to parse the lines read
        ExecutorService consumerPool = Executors.newFixedThreadPool(CONSUMER_COUNT);
        for (int i = 0; i < CONSUMER_COUNT; i++) {
            consumerPool.submit(new Worker(true)); // run method is
            // called
        }

        producerPool.shutdown();
        consumerPool.shutdown();
        while (!producerPool.isTerminated() && !consumerPool.isTerminated()) {
        }

        while (consumerIsDone.size()!= numberOfChunks) {
        }
        mergeParts(nameList, "/Users/deepakpaudel/mycodes/ParallelComputing/dataset/data/merged.txt");
        Date end = new Date();

        long difference = end.getTime() - start.getTime();

        System.out.println("This whole process took: " + difference + " miliseconds.");
    }

    @Override
    public void run() {
        if (isConsumer) {
            consume();
        } else {
            try {
                readAndFragment();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    public static void readAndFragment() throws IOException {
        int CHUNK_SIZE = 69*500000;
//        int CHUNK_SIZE = 141400320;
        File willBeRead = new File("/Users/deepakpaudel/mycodes/ParallelComputing/dataset/household_power_consumption1.txt");
//        File willBeRead = new File("/Users/deepakpaudel/mycodes/ParallelComputing/dataset/test.txt");

        int FILE_SIZE = (int) willBeRead.length();

        System.out.println("Total File Size: " + FILE_SIZE);

        int NUMBER_OF_CHUNKS = 0;
        byte[] temporary = null;

        try {
            InputStream inStream = null;
            int totalBytesRead = 0;

            try {
                inStream = new BufferedInputStream(new FileInputStream(willBeRead));

                while (totalBytesRead < FILE_SIZE) {
                    String PART_NAME = "data" + NUMBER_OF_CHUNKS + ".bin";

                    int bytesRemaining = FILE_SIZE - totalBytesRead;
                    if (bytesRemaining < CHUNK_SIZE) // Remaining Data Part is Smaller Than CHUNK_SIZE
                    // CHUNK_SIZE is assigned to remain volume
                    {
                        CHUNK_SIZE = bytesRemaining;
                        System.out.println("CHUNK_SIZE: " + CHUNK_SIZE);
                    }
                    temporary = new byte[CHUNK_SIZE]; //Temporary Byte Array
                    int bytesRead = inStream.read(temporary, 0, CHUNK_SIZE);

                    if (bytesRead > 0) // If bytes read is not empty
                    {
                        totalBytesRead += bytesRead;
                        NUMBER_OF_CHUNKS++;
                    }
                    chunkQueue.put(temporary); //blocked if reaches its capacity, until consumer consumes
                    lineQueue.put("/Users/deepakpaudel/mycodes/ParallelComputing/dataset/data/" + PART_NAME);
                    nameList.add("/Users/deepakpaudel/mycodes/ParallelComputing/dataset/data/" + PART_NAME);
                    System.out.println("Total Bytes Read: " + totalBytesRead);
                }

            } finally {
                inStream.close();
            }
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }
        producerIsDone = true; // signal consumer
        numberOfChunks = NUMBER_OF_CHUNKS;
        System.out.println("Number of Chunks: "+ NUMBER_OF_CHUNKS);
        System.out.println(Thread.currentThread().getName() + " producer is done");
    }

    private void consume() {
        try {
            while (!producerIsDone || (producerIsDone && !chunkQueue.isEmpty())) {
                byte[] chunkToProcess = chunkQueue.take();
                processCpuDummy(chunkToProcess); // some CPU intensive processing
                System.out.println(Thread.currentThread().getName() + ":: consumer count:" + chunkQueue.size());
                consumerIsDone.add("true");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(Thread.currentThread().getName() + " consumer is done");
    }

    public void processCpuDummy(byte[] chunkToProcess) {
        try {
            String[] lines = readLine(chunkToProcess);

            float defaultValue = 0;
            float numberOfLines = lines.length;
            Map<String, Float> hourActivePower = new HashMap<String, Float>();

            for (String line : lines) {
                String[] data = line.split(";");
                String hour = "";
                if (data.length<2) {
                    continue;
                }
                String[] hour_ = data[1].split(":");
                hour = hour_[0];
                float activePower = Float.parseFloat(data[2]);
                if (hourActivePower.get(hour) == null) {
                    hourActivePower.put(hour, defaultValue);
                    hourActivePower.put("count".concat(hour), defaultValue);
                }
                else {
                    String counter = "count".concat(hour);
                    hourActivePower.put(counter, hourActivePower.get(counter) + 1);
                }
                hourActivePower.put(hour, hourActivePower.get(hour) + activePower);
            }
            String fileName = lineQueue.take();
            write(hourActivePower.toString().getBytes(), fileName);

        } catch (IOException ex) {
            ex.printStackTrace();
        }catch (InterruptedException ex) {
            ex.printStackTrace();
        }
    }

    public static String[] readLine(byte[] chunkToProcess) throws IOException {
        ByteArrayInputStream bis = new ByteArrayInputStream(chunkToProcess);
        int n = bis.available();
        byte[] bytes = new byte[n];
        bis.read(bytes, 0, n);
        String s = new String(bytes, StandardCharsets.UTF_8);
        String[] strArray = s.split("[\\r\\n]+");
        return strArray;
    }

    public static void write(byte[] DataByteArray, String DestinationFileName) {
        try {
            OutputStream output = null;
            try {
                output = new BufferedOutputStream(new FileOutputStream(DestinationFileName));
                output.write(DataByteArray);
                System.out.println("Writing Process Was Performed");
            } finally {
                output.close();
            }
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public static void mergeParts(ArrayList<String> nameList, String DESTINATION_PATH) {
        File[] file = new File[nameList.size()];
        byte AllFilesContent[] = null;

        int TOTAL_SIZE = 0;
        int FILE_NUMBER = nameList.size();
        int FILE_LENGTH = 0;
        int CURRENT_LENGTH = 0;

        for (int i = 0; i < FILE_NUMBER; i++) {
            file[i] = new File(nameList.get(i));
            TOTAL_SIZE += file[i].length();
        }

        try {
            AllFilesContent = new byte[TOTAL_SIZE]; // Length of All Files, Total Size
            InputStream inStream = null;

            for (int j = 0; j < FILE_NUMBER; j++) {
                inStream = new BufferedInputStream(new FileInputStream(file[j]));
                FILE_LENGTH = (int) file[j].length();
                inStream.read(AllFilesContent, CURRENT_LENGTH, FILE_LENGTH);
                CURRENT_LENGTH += FILE_LENGTH;
                inStream.close();
            }

        } catch (FileNotFoundException e) {
            System.out.println("File not found " + e);
        } catch (IOException ioe) {
            System.out.println("Exception while reading the file " + ioe);
        } finally {
            String result = getResult(AllFilesContent);
            write(result.getBytes(StandardCharsets.UTF_8), DESTINATION_PATH);
        }

        System.out.println("Merge was executed successfully.!");

    }

    public static String getResult(byte[] AllFilesContent) {
        String finalData = "";
        try {
            String[] lines = readLine(AllFilesContent);
            float defaultValue = 0;
            String numberOfLines = " numberOfLines";
            Map<String, Float> hourActivePower = new HashMap<String, Float>();
            String nLine = lines[0].replace("}{", ";");
            String nwLines = nLine.replace("{", "");
            String newLine = nwLines.replace("}", "");
            String[] newLines = newLine.split(";");

            for (String line: newLines) {
                for(final String entry : line.split(",")) {
                    final String[] parts = entry.split("=");
                    assert(parts.length == 2) : "Invalid entry: " + entry;
                    float value = Float.parseFloat(parts[1]);
                    if (hourActivePower.get(parts[0]) == null) {
                        hourActivePower.put(parts[0], defaultValue);
                    }
                    hourActivePower.put(parts[0], hourActivePower.get(parts[0]) + value);
                }
            }
            finalData = hourActivePower.toString();
            String counter = "count";
            for (Map.Entry<String, Float> entry : hourActivePower.entrySet()) {
                String key = entry.getKey();
                if (!key.contains(counter.toLowerCase())){
                    String mykey = key.replaceAll("\\s","");
                    String count = " count".concat(mykey);
                    Float value = entry.getValue() / hourActivePower.get(count);
                    hourActivePower.put(key, value);
                }
            }
            finalData = hourActivePower.toString();
            System.out.println(finalData);

        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return finalData;
    }
}