package io.mewbase;


import io.mewbase.server.MewbaseOptions;
import io.vertx.core.json.JsonObject;

import io.vertx.core.net.NetServerOptions;
import io.vertx.core.net.NetServerOptionsConverter;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

/**
 * Created by tim on 30/09/16.
 */
@RunWith(VertxUnitRunner.class)
public class OptionsTest extends MewbaseTestBase {

    private final static Logger logger = LoggerFactory.getLogger(OptionsTest.class);


    @Test
    public void testServerOptions() throws Exception {

        MewbaseOptions options = new MewbaseOptions();
        assertEquals(new NetServerOptions().setPort(MewbaseOptions.DEFAULT_PORT)
                .setIdleTimeout(MewbaseOptions.DEFAULT_CONNECTION_IDLE_TIMEOUT), options.getNetServerOptions());
        NetServerOptions netServerOptions2 = new NetServerOptions();
        options.setNetServerOptions(netServerOptions2);
        assertSame(netServerOptions2, options.getNetServerOptions());

        assertEquals(MewbaseOptions.DEFAULT_BINDERS_DIR, options.getDocsDir());
        String s = randomString();
        options.setDocsDir(s);
        assertEquals(s, options.getDocsDir());

        assertEquals(MewbaseOptions.DEFAULT_MAX_BINDER_SIZE, options.getMaxBinderSize());
        long l = randomLong();
        options.setMaxBinderSize(l);
        assertEquals(l, options.getMaxBinderSize());

        assertEquals(MewbaseOptions.DEFAULT_MAX_BINDERS, options.getMaxBinders());
        int i = randomInt();
        options.setMaxBinders(i);
        assertEquals(i, options.getMaxBinders());
    }

    @Test
    public void testServerOptionsFromEmptyJson() throws Exception {

        JsonObject json = new JsonObject();
        MewbaseOptions options = new MewbaseOptions(json);

        assertEquals(MewbaseOptions.DEFAULT_BINDERS_DIR, options.getDocsDir());
        assertEquals(MewbaseOptions.DEFAULT_MAX_BINDER_SIZE, options.getMaxBinderSize());
        assertEquals(MewbaseOptions.DEFAULT_MAX_BINDERS, options.getMaxBinders());


    }

    @Test
    public void testServerOptionsFromJson() throws Exception {


        // Binders
        String docsDir = randomString();
        long maxBinderSize = randomLong();
        int maxBinders = randomInt();

        // TODO Event Source
        // TODO Event Sink

        // write the Json
        JsonObject json = new JsonObject();
        json.put("docsDir", docsDir);
        json.put("maxBinderSize", maxBinderSize);
        json.put("maxBinders", maxBinders);

        // Inject the options in the Mewbase Object
        MewbaseOptions options = new MewbaseOptions(json);

        // Binders Test
        assertEquals(docsDir, options.getDocsDir());
        assertEquals(maxBinderSize, options.getMaxBinderSize());
        assertEquals(maxBinders, options.getMaxBinders());

    }
}
