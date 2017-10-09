package io.mewbase;

import io.mewbase.server.MewbaseOptions;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RepeatRule;
import org.junit.After;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Random;
import java.util.UUID;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/**
 * Created by tim on 14/10/16.
 */
public class MewbaseTestBase {

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    @Rule
    public RepeatRule repeatRule = new RepeatRule();

    @After
    public void after(TestContext context) throws Exception {
        Thread.sleep(500);
    }


    protected void waitUntil(BooleanSupplier supplier) {
        waitUntil(supplier, 10000);
    }

    protected void waitUntil(BooleanSupplier supplier, long timeout) {
        long start = System.currentTimeMillis();
        while (true) {
            if (supplier.getAsBoolean()) {
                break;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException ignore) {
            }
            long now = System.currentTimeMillis();
            if (now - start > timeout) {
                throw new IllegalStateException("Timed out");
            }
        }
    }

    protected <T> T waitForNonNull(Supplier<T> supplier) {
        return waitForNonNull(supplier, 10000);
    }

    protected <T> T waitForNonNull(Supplier<T> supplier, long timeout) {
        long start = System.currentTimeMillis();
        while (true) {
            T res = supplier.get();
            if (res != null) {
                return res;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException ignore) {
            }
            long now = System.currentTimeMillis();
            if (now - start > timeout) {
                throw new IllegalStateException("Timed out");
            }
        }
    }

    private final Random random = new Random();

    protected int randomInt() {
        return random.nextInt();
    }

    protected long randomLong() {
        return random.nextLong();
    }

    protected String randomString() {
        return UUID.randomUUID().toString();
    }


    protected MewbaseOptions createMewbaseOptions() throws Exception {
        return new MewbaseOptions().setDocsDir(testFolder.newFolder().getPath());
    }
}
