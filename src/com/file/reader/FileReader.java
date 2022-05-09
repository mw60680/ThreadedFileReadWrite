package com.file.reader;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class FileReader {
    public static void main(String[] args) throws InterruptedException {
        String path = "C:\\Project\\holodeck\\data\\ol_dump_ratings_2022-03-29.txt";
        long start = System.currentTimeMillis();
        List<Reader> threads = new ArrayList<>();
        final int threadCount = 5;

        try {
            long breakpoint = 0;

            for (int i = 1; i <= threadCount; i++) {
                RandomAccessFile file = new RandomAccessFile(path, "r");
                file.seek(breakpoint);
                if (breakpoint != 0) {
                    file.readLine();
                }
                threads.add(new Reader(file, (file.length()/threadCount)*i));
                breakpoint = (file.length()/threadCount) * i;
            }
//            RandomAccessFile randomAccessFile = new RandomAccessFile(path, "r");
//            threads.add(new Reader(randomAccessFile, randomAccessFile.length()/2));
//
//            RandomAccessFile randomAccessFile1 = new RandomAccessFile(path, "r");
//            long someval = (randomAccessFile1.length()/4) + 1;
//            randomAccessFile1.seek(someval);
//            threads.add(new Reader(randomAccessFile1, randomAccessFile.length()/2));

            for (Reader reader: threads) reader.start();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        threads.get(0).join();
        threads.get(1).join();
        threads.get(2).join();
        threads.get(3).join();
        threads.get(4).join();
        long end = System.currentTimeMillis();

        System.out.println(end - start);
    }
}

class Reader extends Thread {
    private final RandomAccessFile randomAccessFile;
    private List<String> lines;
    private long limit;
    private static String outPath = "C:\\Project\\holodeck\\data\\sample-output.txt";
    private static RandomAccessFile outFIle;

    static {
        try {
            outFIle = new RandomAccessFile(outPath, "rw");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public Reader(RandomAccessFile randomAccessFile, long limit) throws IOException {
        this.randomAccessFile = randomAccessFile;
        this.limit = limit;
        lines = new ArrayList<>();
    }

    @Override
    public void run() {
        try {
            long currentPos = randomAccessFile.getFilePointer();
            String line = null;
            int count = 0;
            int batch = 100;
            List<String> list = new ArrayList<>();

            while (currentPos < limit) {
                count++;
                line = randomAccessFile.readLine();
                list.add(line);

                if (count == batch) {
//                    synchronized (this) {
                        outFIle.writeChars(String.join("\n", list) + "\n");
//                    }

                    list.clear();
                    count = 0;
                }

                System.out.println(line);

                currentPos = randomAccessFile.getFilePointer();
            }

            if (count > 0) {
                outFIle.writeChars(String.join("\n", list) + "\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public List<String> getLines() {return this.lines;}
}
