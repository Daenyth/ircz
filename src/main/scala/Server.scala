import java.net.InetSocketAddress
import java.nio.channels.AsynchronousChannelGroup
import java.util.concurrent.{ExecutorService, Executors}

import fs2.async.mutable.{Queue, Signal}
import fs2.io.tcp
import fs2.io.tcp.Socket
import fs2.util.Async
import fs2.{Pipe, Sink, Strategy, Stream, Task, async}

import scala.concurrent.duration._

object Server {
  val maxThreads = 16
  val tcpThreads = 8

  def address = new InetSocketAddress("127.0.0.1", 9090)

  val streamPool: ExecutorService = Executors.newFixedThreadPool(maxThreads)
  implicit val S: Strategy = Strategy.fromExecutor(streamPool)
  implicit val acg: AsynchronousChannelGroup =
    AsynchronousChannelGroup.withThreadPool(
      Executors.newFixedThreadPool(tcpThreads))

  type Listener = Sink[Task, Byte]

  // The mutable server state:
  // Contains the set of sinks to send messages to.
  val clientsT: Task[Signal[Task, Set[Listener]]] =
    async.signalOf[Task, Set[Listener]](Set.empty)

  // Add a client to the signal
  def addClient(clients: Signal[Task, Set[Server.Listener]],
                c: Listener): Task[Async.Change[Set[Listener]]] =
    clients.modify { _ + c }

  // Remove a client from the signal
  def removeClient(clients: Signal[Task, Set[Server.Listener]],
                   c: Listener): Task[Async.Change[Set[Listener]]] =
    clients.modify { _ - c }

  // The netty server as a stream of incoming connections
  val connections: Stream[Task, Stream[Task, Socket[Task]]] =
    tcp.server[Task](address)

  // The queue of messages from clients
  val messageQueueT: Task[Queue[Task, Byte]] =
    async.boundedQueue[Task, Byte](8192)

  // The process that serves clients.
  // Establishes client connections, registers them as listeners,
  // and enqueues all their messages on the relay,
  // stripping any client errors.
  def serve(mq: Queue[Task, Byte],
            clientSignal: Signal[Task, Set[Listener]]): Stream[Task, Unit] =
    fs2.concurrent.join(maxThreads)(connections map { client =>
      for {
        c <- client
        timeout = 10.seconds
        src = c.reads(maxBytes = 256, Some(timeout))
        snk = c.writes(Some(timeout))
        _ <- Stream.bracket(addClient(clientSignal, snk))(
          _ => src to mq.enqueue,
          _ => removeClient(clientSignal, snk).map(_ => ()))
      } yield ()
    })

  // The process that relays messages to clients.
  // For each message on the queue, get all clients.
  // For each client, send the message to the client.
  // If that fails, remove the client.
  def relay(messageQueue: Queue[Task, Byte],
            clients: Signal[Task, Set[Listener]]): Stream[Task, Unit] =
    for {
      message <- messageQueue.dequeue.through(log("relay"))
      cs <- Stream.eval(clients.get)
      client <- Stream.emits(cs.toSeq)
      _ <- Stream.emit(message) to client
    } yield ()

  // The main server process
  def mainStream(
      mq: Queue[Task, Byte],
      clientSignal: Signal[Task, Set[Listener]]): Stream[Task, Unit] =
    serve(mq, clientSignal) mergeHaltBoth relay(mq, clientSignal)

  val mainTask: Task[Unit] =
    for {
      mq <- messageQueueT
      clientSignal <- clientsT
      stream <- mainStream(mq, clientSignal).run
    } yield stream

  def log[A](prefix: String): Pipe[Task, A, A] =
    _.evalMap { a =>
      Task.delay { println(s"$prefix> $a"); a }
    }

  def main(args: Array[String]): Unit = mainTask.unsafeRun
}
