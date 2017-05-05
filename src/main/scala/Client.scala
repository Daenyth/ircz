import java.net.InetSocketAddress
import java.nio.channels.AsynchronousChannelGroup
import java.util.concurrent.{ExecutorService, Executors}

import fs2.{Strategy, Stream, Task, io, text}
import fs2.io.tcp

object Client {

  def address = new InetSocketAddress("localhost", 9090)

  val pool: ExecutorService = Executors.newFixedThreadPool(4)
  implicit val S: Strategy = Strategy.fromExecutor(pool)
  implicit val acg: AsynchronousChannelGroup =
    AsynchronousChannelGroup.withThreadPool(pool)

  val mainProcess: Stream[Task, Unit] = for {
    // Get the user's nick
    _ <- Stream
      .emit("** Enter your nick: **\n")
      .covary[Task] through text.utf8Encode to io.stdout
    nick <- io.stdin[Task](64) through text.utf8Decode through text.lines take 1

    // Connect to the server
    c <- tcp.client[Task](address)
    _ <- Stream
      .emit(s"** Connected as $nick **\n")
      .covary[Task] through text.lines through text.utf8Encode to io.stdout

    // Reads UTF8 bytes from the connection, decodes them, and sends to stdout
    in = c.reads(256) through text.utf8Decode through text.lines through text.utf8Encode to io.stdout

    // Reads from stdin, prepends the nick, encodes as UTF8, and sends to the server
    out = io
      .stdin[Task](64)
      .through(text.utf8Decode)
      .through(text.lines)
      .map { msg =>
        s"$nick: $msg"
      }
      .through(text.lines)
      .through(text.utf8Encode)
      .to(c.writes())

    // The main process nondeterministically merges the in and out processes
    _ <- in mergeHaltBoth out
  } yield ()

  val mainTask = mainProcess.run

  def main(args: Array[String]): Unit = mainTask.unsafeRun
}
