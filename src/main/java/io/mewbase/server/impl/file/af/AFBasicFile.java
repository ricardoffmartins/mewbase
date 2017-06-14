package io.mewbase.server.impl.file.af;

import io.mewbase.server.impl.BasicFile;
import io.mewbase.util.AsyncResCF;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.impl.AsyncFileImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 11/10/16.
 */
public class AFBasicFile implements BasicFile {

    private final static Logger logger = LoggerFactory.getLogger(AFBasicFile.class);

    private final AsyncFile af;

    public AFBasicFile(AsyncFile af) {
        this.af = af;
    }

    @Override
    public synchronized CompletableFuture<Void> append(Buffer buffer, int writePos) {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        af.write(buffer, writePos, ar -> {
            if (ar.succeeded()) {
                cf.complete(null);
            } else {
                cf.completeExceptionally(ar.cause());
            }
        });
        return cf;
    }

    @Override
    public CompletableFuture<Void> read(Buffer buffer, int length, int readPos) {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        af.read(buffer, 0, readPos, length, ar -> {
            if (ar.succeeded()) {
                cf.complete(null);
            } else {
                cf.completeExceptionally(ar.cause());
            }
        });
        return cf;
    }

    /*
    Workaround for https://github.com/eclipse/vert.x/issues/2012
    Can be removed once it's fixed
    We're going to call the private doClose method directly to force a close of the file and prevent a hang
     */
    private static Method AFI_DOCLOSE_METHOD;
    static {
        try {
            AFI_DOCLOSE_METHOD = AsyncFileImpl.class.getDeclaredMethod("doClose", Handler.class);
            AFI_DOCLOSE_METHOD.setAccessible(true);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    public CompletableFuture<Void> close() {
        AsyncResCF<Void> ar = new AsyncResCF<>();
        af.flush(res -> {
            if (res.succeeded()) {
                try {
                    Handler<AsyncResult<Void>> handler = res2 -> {
                        if (res2.succeeded()) {
                            ar.complete(res2.result());
                        } else {
                            ar.completeExceptionally(res2.cause());
                        }
                    };
                    AFI_DOCLOSE_METHOD.invoke(af, handler);
                } catch (Exception e) {
                    ar.completeExceptionally(e);
                }
            } else {
                ar.completeExceptionally(res.cause());
            }
        });
        return ar;
    }

    public CompletableFuture<Void> flush() {
        AsyncResCF<Void> ar = new AsyncResCF<>();
        af.flush(ar);
        return ar;
    }
}
