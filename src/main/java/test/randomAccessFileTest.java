package test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class randomAccessFileTest {
    public static void main(String args[]) {
        test();
    }

    private static void test() {

        double d = 1.5987475;

        // create a new RandomAccessFile with filename test
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile("data/test.txt", "rw");

            // write something in the file
            raf.writeDouble(765.497634);

            // set the file pointer at 0 position
            raf.seek(0);

            // read double
            System.out.println("" + raf.readDouble());

            // set the file pointer at 0 position
            raf.seek(0);

            // write a double at the start
            raf.writeDouble(d);

            // set the file pointer at 0 position
            raf.seek(0);

            // read double
            System.out.println("" + raf.readDouble());

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
