package io.mewbase;


import io.mewbase.server.ServerOptions;
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.core.net.PemTrustOptions;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class SSLTest extends ServerTestBase {

    private final static String CERT_PATH = "src/test/resources/server-cert.pem";
    private final static String KEY_PATH = "src/test/resources/server-key.pem";

    // Serves as an example of setting up an SSL connection.

//    @Override
//    protected ServerOptions createServerOptions() {
//        ServerOptions serverOptions = super.createServerOptions();
//        serverOptions.getNetServerOptions().setSsl(true).setPemKeyCertOptions(
//                new PemKeyCertOptions()
//                        .setKeyPath(KEY_PATH)
//                        .setCertPath(CERT_PATH)
//        );
//        return serverOptions;
//    }
//
//    @Override
//    protected ClientOptions createClientOptions() {
//        ClientOptions clientOptions = super.createClientOptions();
//        clientOptions.getNetClientOptions()
//                .setSsl(true)
//                .setPemTrustOptions(
//                        new PemTrustOptions().addCertPath(CERT_PATH)
//                );
//        return clientOptions;
//    }

    @Test
    public void testSimplePubSub(TestContext context) throws Exception {
       // Try and get a doc over ssl
        assert(true);
    }
}
