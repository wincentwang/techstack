package wang.wincent.techstack.threadio.netty.aio;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;

/**
 * @version 1.0
 */
public class AcceptCompletionHandler implements

	CompletionHandler<AsynchronousSocketChannel, AsyncTimeServerHandler> {


    public void completed(AsynchronousSocketChannel result,
	    AsyncTimeServerHandler attachment) {
		attachment.asynchronousServerSocketChannel.accept(attachment, this);
		ByteBuffer buffer = ByteBuffer.allocate(1024);
		result.read(buffer, buffer, new ReadCompletionHandler(result));
    }


    public void failed(Throwable exc, AsyncTimeServerHandler attachment) {
	exc.printStackTrace();
	attachment.latch.countDown();
    }

}
