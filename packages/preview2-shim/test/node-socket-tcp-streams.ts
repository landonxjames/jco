import { throws } from "node:assert";
import { setTimeout as sleep } from "node:timers/promises";
import { Worker } from "node:worker_threads";

import { sockets } from "@bytecodealliance/preview2-shim";
import { assert, suite, test } from "vitest";

import type {
  InputStream,
  OutputStream,
  StreamError,
} from "../types/interfaces/wasi-io-streams.js";
import type { TcpSocket } from "../types/interfaces/wasi-sockets-tcp.js";

const TIMEOUT_MS = 10_000;

const encoder = new TextEncoder();
const decoder = new TextDecoder();

interface Connection {
  socket: TcpSocket;
  input: InputStream;
  output: OutputStream;
}

interface ServerThread {
  port: number;
  nextReport(): Promise<unknown>;
  close(): Promise<void>;
}

/**
 * Starts a `node:net` server on a worker thread. The shim's blocking calls
 * block this thread's event loop, so a server on this thread could not
 * answer them
 *
 * @param handleConnection - Source of a function that the worker calls with
 *   each accepted socket and a `report` function that sends a value to this
 *   thread
 * @returns The server's port and access to its reports
 */
async function startServerThread(
  handleConnection: string,
): Promise<ServerThread> {
  const worker = new Worker(
    `
        const net = require("node:net");
        const { parentPort } = require("node:worker_threads");
        const handleConnection = ${handleConnection};
        const report = (value) => parentPort.postMessage({ report: value });
        const server = net.createServer({ allowHalfOpen: true }, (socket) =>
            handleConnection(socket, report),
        );
        server.listen(0, "127.0.0.1", () => parentPort.postMessage({ port: server.address().port }));
        `,
    { eval: true },
  );

  // Queue reports until the test asks for them
  const reports: unknown[] = [];
  let reportArrived: (() => void) | null = null;
  const port = await new Promise<number>((resolve, reject) => {
    worker.once("error", reject);
    worker.on("message", (message: { port?: number; report?: unknown }) => {
      if (message.port !== undefined) {
        resolve(message.port);
        return;
      }
      reports.push(message.report);
      reportArrived?.();
    });
  });

  return {
    port,
    async nextReport() {
      if (reports.length === 0) {
        await new Promise<void>((resolve, reject) => {
          const timer = setTimeout(
            () => reject(new Error("timed out waiting for the server thread")),
            TIMEOUT_MS,
          );
          reportArrived = () => {
            clearTimeout(timer);
            resolve();
          };
        });
        reportArrived = null;
      }
      return reports.shift();
    },
    async close() {
      await worker.terminate();
    },
  };
}

/**
 * Connects a shim TCP socket to a port on the loopback address
 *
 * @param port - Port to connect to
 * @returns The connected socket and its streams
 */
function connect(port: number): Connection {
  const network = sockets.instanceNetwork.instanceNetwork();
  const socket = sockets.tcpCreateSocket.createTcpSocket("ipv4");
  socket.startConnect(network, {
    tag: "ipv4",
    val: { address: [127, 0, 0, 1], port },
  });
  waitForPollable(socket);
  const [input, output] = socket.finishConnect();
  return { socket, input, output };
}

/**
 * Connects a shim TCP socket to a shim listener, so that the test controls
 * both ends of the connection
 *
 * @returns Both ends of the connection
 */
function connectPair(): { client: Connection; server: Connection } {
  const network = sockets.instanceNetwork.instanceNetwork();
  const listener = sockets.tcpCreateSocket.createTcpSocket("ipv4");
  listener.startBind(network, {
    tag: "ipv4",
    val: { address: [127, 0, 0, 1], port: 0 },
  });
  waitForPollable(listener);
  listener.finishBind();
  listener.startListen();
  waitForPollable(listener);
  listener.finishListen();

  const localAddress = listener.localAddress();
  assert(localAddress.tag === "ipv4");
  const client = connect(localAddress.val.port);

  waitForPollable(listener);
  const [socket, input, output] = listener.accept();
  return { client, server: { socket, input, output } };
}

/**
 * Blocks until a resource's pollable is ready, then drops the pollable
 *
 * @param resource - Socket or stream to wait for
 */
function waitForPollable(resource: TcpSocket | InputStream) {
  const pollable = resource.subscribe();
  pollable.block();
  pollable[Symbol.dispose]();
}

/**
 * Reads an input stream until it reports an error. The reads do not block,
 * so a stream that never ends fails the test instead of blocking the thread
 *
 * @param input - Stream to read
 * @param chunkSize - Most bytes to request per read
 * @returns The bytes read, as text, and the error that ended the stream
 */
async function readUntilError(
  input: InputStream,
  chunkSize = 1024n,
): Promise<{ text: string; error: StreamError }> {
  const deadline = Date.now() + TIMEOUT_MS;
  let text = "";
  for (;;) {
    let chunk: Uint8Array;
    try {
      chunk = input.read(chunkSize);
    } catch (error) {
      return { text, error: error as StreamError };
    }
    text += decoder.decode(chunk, { stream: true });
    if (chunk.byteLength === 0) {
      if (Date.now() > deadline) {
        throw new Error(`timed out reading after ${JSON.stringify(text)}`);
      }
      await sleep(10);
    }
  }
}

/**
 * Asserts that a stream operation reports `closed`
 *
 * @param operation - Operation to run
 */
function assertClosed(operation: () => unknown) {
  throws(operation, (err: StreamError) => err?.tag === "closed");
}

suite("TCP streams", () => {
  test("input-stream.read returns bytes received before a connection reset", async () => {
    const response = "HTTP/1.1 503 Slow Down\r\ncontent-length: 0\r\n\r\n";
    const server = await startServerThread(`(socket) => {
            socket.write(${JSON.stringify(response)});
            socket.once("data", () => socket.resetAndDestroy());
        }`);
    try {
      const { input, output } = connect(server.port);
      waitForPollable(input);
      // The response is now buffered. Ask the server to reset the
      // connection, then wait until the shim has seen the reset. The
      // output stream shares the socket, so it reports the reset too
      output.blockingWriteAndFlush(encoder.encode("reset"));
      const deadline = Date.now() + TIMEOUT_MS;
      for (;;) {
        try {
          output.checkWrite();
        } catch {
          break;
        }
        assert(Date.now() < deadline, "timed out waiting for the reset");
        await sleep(10);
      }

      // Small reads leave buffered bytes behind after the first read
      const { text, error } = await readUntilError(input, 16n);
      assert.strictEqual(text, response);
      assert(error.tag === "last-operation-failed");
      // The shim's `wasi:io/error` resource carries the Node.js error
      const { payload } = error.val as unknown as {
        payload: NodeJS.ErrnoException;
      };
      assert.strictEqual(payload.code, "ECONNRESET");
    } finally {
      await server.close();
    }
  });

  test("tcp-socket.shutdown(send) keeps the receive side open", async () => {
    const { client, server } = connectPair();

    client.output.blockingWriteAndFlush(encoder.encode("ping"));
    client.socket.shutdown("send");
    assertClosed(() => client.output.checkWrite());
    assertClosed(() => client.output.write(encoder.encode("late")));

    assert.deepStrictEqual(await readUntilError(server.input), {
      text: "ping",
      error: { tag: "closed" },
    });

    // Reply over the half-closed connection, which also checks the
    // accepted socket's shutdown
    server.output.blockingWriteAndFlush(encoder.encode("pong"));
    server.socket.shutdown("send");
    assert.deepStrictEqual(await readUntilError(client.input), {
      text: "pong",
      error: { tag: "closed" },
    });
  });

  test("tcp-socket.shutdown(receive) discards input and keeps the send side open", async () => {
    const server = await startServerThread(`(socket, report) => {
            socket.write("buffered before shutdown");
            let received = "";
            socket.setEncoding("utf8");
            socket.on("data", (data) => (received += data));
            socket.on("end", () => {
                // This write finishes only if the client keeps reading after
                // it shut down its receive side.
                socket.end(Buffer.alloc(32 * 1024 * 1024), (err) =>
                    report({ received, error: err?.code ?? null }),
                );
            });
        }`);
    try {
      const { socket, input, output } = connect(server.port);
      waitForPollable(input);
      socket.shutdown("receive");
      assertClosed(() => input.read(1024n));
      assertClosed(() => input.blockingRead(1024n));

      output.blockingWriteAndFlush(encoder.encode("hello"));
      socket.shutdown("send");
      assert.deepStrictEqual(await server.nextReport(), {
        received: "hello",
        error: null,
      });
    } finally {
      await server.close();
    }
  });

  test("tcp-socket.shutdown(both) closes both streams after sending written bytes", async () => {
    const { client, server } = connectPair();

    client.output.blockingWriteAndFlush(encoder.encode("bye"));
    client.socket.shutdown("both");
    // Shutting down a direction again has no effect
    client.socket.shutdown("both");
    client.socket.shutdown("receive");
    assertClosed(() => client.input.read(1024n));
    assertClosed(() => client.output.checkWrite());

    assert.deepStrictEqual(await readUntilError(server.input), {
      text: "bye",
      error: { tag: "closed" },
    });
  });
});
