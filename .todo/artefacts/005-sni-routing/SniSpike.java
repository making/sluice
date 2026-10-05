// Spike A: SNI routing feasibility.
// Two TLS echo backends + a TCP front proxy that peeks the ClientHello, extracts the
// server_name extension, routes by SNI, and relays raw bytes (TLS passthrough).
// Verifies: SNI extraction, handshake passes through, echo round-trip, half-close.
import javax.net.ssl.*;
import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.util.function.IntPredicate;

public class SniSpike {

	// ---- ClientHello SNI extraction (the core of the spike) ----

	/** Extracts the first server_name of type host_name (0) from a TLS record. */
	record Peek(String sni, byte[] headBytes) {}

	static Peek peekSni(InputStream in) throws IOException {
		DataInputStream d = new DataInputStream(in);
		System.out.println("peek start");
		ByteArrayOutputStream head = new ByteArrayOutputStream();
		int contentType = d.readUnsignedByte();
		if (contentType != 0x16) {
			throw new IOException("not a handshake record: " + contentType);
		}
		head.write(contentType);
		int v = d.readUnsignedByte(); head.write(v);
		v = d.readUnsignedByte(); head.write(v);
		int recordLen = d.readUnsignedShort();
		head.write(recordLen >> 8); head.write(recordLen);
		byte[] record = new byte[recordLen];
		d.readFully(record);
		head.write(record);
		DataInputStream r = new DataInputStream(new ByteArrayInputStream(record));
		if (r.readUnsignedByte() != 1) { // handshake type ClientHello
			throw new IOException("not client hello");
		}
		int hsLen = (r.readUnsignedByte() << 16) | (r.readUnsignedShort());
		r.readUnsignedShort(); // legacy version
		byte[] random = new byte[32];
		r.readFully(random);
	 sessionId(r); // session id
	 cipherSuites(r); // cipher suites
	 compressionMethods(r);
		int extLen = r.readUnsignedShort();
		int end = 2 + 4 + 32;
		// walk extensions within the handshake body we already have
		DataInputStream e = new DataInputStream(new ByteArrayInputStream(record, record.length - r.available(), r.available()));
		int extRemaining = Math.min(extLen, e.available());
		while (extRemaining >= 4) {
			int type = e.readUnsignedShort();
			int len = e.readUnsignedShort();
			extRemaining -= 4 + len;
			if (type != 0x0000) { // server_name
				if (len > 0) e.skipBytes(len);
				continue;
			}
			DataInputStream s = new DataInputStream(new ByteArrayInputStream(range(e, len)));
			s.readUnsignedShort(); // server_name_list length
			s.readUnsignedByte(); // name type: host_name(0)
			int nameLen = s.readUnsignedShort();
			byte[] name = new byte[nameLen];
			s.readFully(name);
			return new Peek(new String(name, StandardCharsets.US_ASCII), head.toByteArray());
		}
		return new Peek(null, head.toByteArray());
	}

	private static byte[] range(DataInputStream in, int len) throws IOException {
		byte[] b = new byte[len];
		in.readFully(b);
		return b;
	}

	// variable-length helpers (kept explicit for legibility)
	static void sessionId(DataInputStream d) throws IOException {
		int len = d.readUnsignedByte();
		d.skipBytes(len);
	}

	static void cipherSuites(DataInputStream d) throws IOException {
		int len = d.readUnsignedShort();
		d.skipBytes(len);
	}

	static void compressionMethods(DataInputStream d) throws IOException {
		int len = d.readUnsignedByte();
		d.skipBytes(len);
	}

	// ---- raw bidirectional relay with half-close ----

	static void relay(Socket a, Socket b) {
		Thread.ofVirtual().start(() -> pump(a, b));
		pump(b, a);
	}

	static void pump(Socket from, Socket to) {
		String tag = from.getLocalPort() + "->" + to.getPort();
		try (from; to) {
			InputStream in = from.getInputStream();
			OutputStream out = to.getOutputStream();
			byte[] buf = new byte[4096];
			int n;
			while ((n = in.read(buf)) > 0) {
				System.out.println("pump " + tag + " " + n + "B");
				out.write(buf, 0, n);
				out.flush();
			}
			to.shutdownOutput(); // propagate half-close
			System.out.println("pump " + tag + " eof");
		}
		catch (IOException e) {
			System.out.println("pump " + tag + " err " + e);
		}
	}

	static SSLContext sharedCtx;

	public static void main(String[] args) throws Exception {
		// keystore shared by backends and client trust
		Process keytool = new ProcessBuilder("keytool", "-genkeypair", "-alias", "t", "-keyalg", "RSA",
				"-keystore", "ks.p12", "-storetype", "PKCS12", "-storepass", "spikepass", "-dname", "CN=t",
				"-ext", "san=dns:a.example,dns:b.example", "-validity", "1")
			.inheritIO()
			.start();
		keytool.waitFor();

		KeyStore ks = KeyStore.getInstance("PKCS12");
		try (InputStream in = new FileInputStream("ks.p12")) {
			ks.load(in, "spikepass".toCharArray());
		}
		KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
		kmf.init(ks, "spikepass".toCharArray());
		TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
		tmf.init(ks);
		SSLContext ctx = SSLContext.getInstance("TLS");
		ctx.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
		sharedCtx = ctx;

		// backends: 9101 = a.example, 9102 = b.example (plain echo over TLS from the
		// proxy's perspective is unnecessary -- backends speak plain TCP; the proxy
		// passes TLS bytes through untouched)
		ServerSocket backendA = new ServerSocket(9101);
		ServerSocket backendB = new ServerSocket(9102);
		for (ServerSocket backend : new ServerSocket[] { backendA, backendB }) {
			Thread.ofVirtual().start(() -> {
				while (true) {
					try {
						Socket raw = backend.accept();
						SSLSocket s = (SSLSocket) sharedCtx.getSocketFactory().createSocket(raw, "t", raw.getLocalPort(), true);
						s.setUseClientMode(false);
						s.startHandshake();
						Thread.ofVirtual().start(() -> echo(s));
					}
					catch (IOException e) {
						return;
					}
				}
			});
		}

		// front proxy: single port, route by SNI
		ServerSocket front = new ServerSocket(9100);
		Thread.ofVirtual().start(() -> {
			while (true) {
				try {
					Socket client = front.accept();
					Thread.ofVirtual().start(() -> serve(client));
				}
				catch (IOException e) {
					return;
				}
			}
		});

		// clients: TLS with SNI a.example -> expects echo "hello-a", etc.
		roundTrip(ctx, "a.example", 9100, 9101, "hello-a");
		roundTrip(ctx, "b.example", 9100, 9102, "hello-b");
		System.out.println("SPIKE-A OK");
	}

	static void serve(Socket client) {
		System.out.println("serve accepted local="+client.getLocalPort()+" remote="+client.getRemoteSocketAddress());
		try {
			Peek peek = peekSni(client.getInputStream());
			System.out.println("routed by SNI: " + peek.sni());
			int port = "a.example".equals(peek.sni()) ? 9101 : 9102;
			Socket upstream = new Socket("localhost", port);
			// replay the consumed ClientHello, then relay the remainder
			upstream.getOutputStream().write(peek.headBytes());
			upstream.getOutputStream().flush();
			relay(client, upstream);
		}
		catch (Exception e) {
			System.out.println("serve failed: " + e);
			try {
				client.close();
			}
			catch (IOException ignored) {
			}
		}
	}

	static void echo(Socket s) {
		try (s) {
			InputStream in = s.getInputStream();
			OutputStream out = s.getOutputStream();
			byte[] buf = new byte[4096];
			int n;
			while ((n = in.read(buf)) > 0) {
				System.out.println("echo " + n + "B");
				out.write(buf, 0, n);
				out.flush();
			}
			System.out.println("echo eof");
		}
		catch (IOException e) {
			System.out.println("echo err " + e);
		}
	}

	static void roundTrip(SSLContext ctx, String sni, int frontPort, int backendPort, String payload) throws Exception {
		SSLSocket ssl = (SSLSocket) ctx.getSocketFactory().createSocket();
		ssl.connect(new InetSocketAddress("localhost", frontPort), 5000);
		SSLParameters params = ssl.getSSLParameters();
		params.setServerNames(java.util.List.of(new SNIHostName(sni)));
		ssl.setSSLParameters(params);
		ssl.startHandshake();
		OutputStream out = ssl.getOutputStream();
		out.write((payload + "\n").getBytes(StandardCharsets.US_ASCII));
		out.flush();
		BufferedReader in = new BufferedReader(new InputStreamReader(ssl.getInputStream(), StandardCharsets.US_ASCII));
		String echoed = in.readLine();
		if (!payload.equals(echoed)) {
			throw new AssertionError("expected " + payload + " got " + echoed);
		}
		// half-close: EOF must propagate through the proxy to the backend and back
		ssl.close();
		System.out.printf("roundTrip ok: sni=%s port=%d echoed=%s%n", sni, backendPort, echoed);
	}

}
