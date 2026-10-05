package am.ik.sluice.exampleupstream;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.http2.server.HTTP2CServerConnectionFactory;
import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.util.Callback;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

/**
 * Minimal sample upstream for manual checks: answers "It works" over HTTP/1.1 and h2c
 * (HTTP/2 prior knowledge) on the same port.
 *
 * Usage: java -jar sluice-example-upstream-*-exec.jar [port]
 */
public final class ExampleUpstream {

	private static final String BODY = "It works";

	private ExampleUpstream() {
	}

	public static void main(String[] args) throws Exception {
		int port = args.length > 0 ? Integer.parseInt(args[0]) : 8080;
		Server server = new Server(new QueuedThreadPool());
		HttpConfiguration httpConfiguration = new HttpConfiguration();
		// HTTP/1.1 and h2c (cleartext HTTP/2 prior knowledge) on the same port
		ConnectionFactory http1 = new HttpConnectionFactory(httpConfiguration);
		HTTP2CServerConnectionFactory http2c = new HTTP2CServerConnectionFactory(httpConfiguration);
		ServerConnector connector = new ServerConnector(server, http1, http2c);
		connector.setPort(port);
		server.addConnector(connector);
		ContextHandler context = new ContextHandler("/");
		context.setHandler(new Handler.Abstract() {
			@Override
			public boolean handle(Request request, Response response, Callback callback) throws Exception {
				response.setStatus(HttpStatus.OK_200);
				response.getHeaders().put("Content-Type", "text/plain;charset=utf-8");
				response.write(true, ByteBuffer.wrap(BODY.getBytes(StandardCharsets.UTF_8)), callback);
				return true;
			}
		});
		server.setHandler(context);
		server.start();
		System.out.println("example upstream listening on :" + port + " (http/1.1 + h2c)");
	}

}
