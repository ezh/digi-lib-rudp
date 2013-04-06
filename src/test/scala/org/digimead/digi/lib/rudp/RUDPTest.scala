/**
 * Digi-Lib-RUDP - Reliable UDP protocol for Digi components
 *
 * Copyright (c) 2012-2013 Alexey Aksenov ezh@ezh.msk.ru
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.digimead.digi.lib.rudp

import java.net.InetSocketAddress
import java.util.concurrent.Executors

import org.digimead.digi.lib.DependencyInjection
import org.digimead.digi.lib.log.Loggable
import org.digimead.digi.lib.log.Logging
import org.digimead.digi.lib.log.logger.RichLogger.rich2slf4j
import org.digimead.lib.test.TestHelperLogging
import org.digimead.lib.test.TestHelperMatchers
import org.jboss.netty.bootstrap.ConnectionlessBootstrap
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.channel.Channel
import org.jboss.netty.channel.ChannelFuture
import org.jboss.netty.channel.ChannelFutureListener
import org.jboss.netty.channel.ChannelHandlerContext
import org.jboss.netty.channel.ChannelPipeline
import org.jboss.netty.channel.ChannelPipelineFactory
import org.jboss.netty.channel.ChannelStateEvent
import org.jboss.netty.channel.Channels
import org.jboss.netty.channel.ExceptionEvent
import org.jboss.netty.channel.MessageEvent
import org.jboss.netty.channel.SimpleChannelHandler
import org.jboss.netty.channel.socket.nio.NioDatagramChannelFactory
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder
import org.jboss.netty.handler.execution.ExecutionHandler
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor
import org.jboss.netty.logging.InternalLoggerFactory
import org.jboss.netty.logging.Slf4JLoggerFactory
import org.jboss.netty.util.CharsetUtil
import org.scalatest.BeforeAndAfter
import org.scalatest.fixture.FunSuite
import org.scalatest.matchers.ShouldMatchers

class RUDPTest_j1 extends FunSuite with ShouldMatchers with BeforeAndAfter with TestHelperLogging with TestHelperMatchers {
  type FixtureParam = Map[String, Any]

  override def withFixture(test: OneArgTest) {
    DependencyInjection.get.foreach(_ => DependencyInjection.clear)
    DependencyInjection.set(defaultConfig(test.configMap), { Logging })
    withLogging(test.configMap) {
      test(test.configMap)
    }
  }

  test("client") {
    config =>
      InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory())
      val threadPool = Executors.newCachedThreadPool()
      val factory = new NioDatagramChannelFactory(threadPool)
      val bootstrap = new ConnectionlessBootstrap(factory)
      bootstrap.setOption("reuseAddress", true)
      bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
        def getPipeline(): ChannelPipeline = {
          val pipeline = Channels.pipeline()
          pipeline.addFirst("execution-handler", new ExecutionHandler(
            new OrderedMemoryAwareThreadPoolExecutor(16, 1048576, 1048576)))
          pipeline.addLast("RUDP frame handler", new RUDPFrameHandler)
          pipeline.addLast("RUDP data handler", new RUDPDataHandler)
          pipeline.addLast("handler", new ClientChannelHandler)
          pipeline
        }
      })
      val future = bootstrap.connect(RUDPTest.address)
      val channel = future.awaitUninterruptibly().getChannel()
      log.debug("start client")
      future.isSuccess() should be(true)
      //channel.write("1234567890")
      Thread.sleep(5000)
      log.debug("stop client")
      future.getChannel().close().awaitUninterruptibly()
      bootstrap.releaseExternalResources()
  }
  class ClientChannelHandler extends SimpleChannelHandler with Loggable {
    override def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
      log.debug("connected")
      val ch = e.getChannel()
      log.___glance("send " + RUDPTest.data)
      val data = ChannelBuffers.copiedBuffer(RUDPTest.data, CharsetUtil.ISO_8859_1)
      val f = ch.write(data)
      f.addListener(new ChannelFutureListener() {
        def operationComplete(future: ChannelFuture) {
          val ch = future.getChannel()
          log.___glance("DONE: " + future.isSuccess())
          ch.close()
        }
      })
      super.channelConnected(ctx, e)
    }
    override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) = e.getMessage() match {
      case message: String =>
        log.debug("message " + e.getMessage())
      case message =>
        // Note: in theory, we can't get here, because the
        // codec would have thrown an exception beforehand
        log.fatal("Received unknown message object: " + message.getClass())
    }
    override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
      log.error("Client exception: " + e.getCause(), e.getCause())
    }
  }

  class ClientChannelDecoder extends OneToOneDecoder with Loggable {
    override protected def decode(ctx: ChannelHandlerContext, channel: Channel, msg: Object): Object = msg match {
      case msg: ChannelBuffer =>
        log.debug("DECODE " + msg)
        msg
      case msg =>
        log.error("Unknown message object class: " + msg.getClass())
        msg
    }
  }
}

class RUDPTest_j2 extends FunSuite with ShouldMatchers with BeforeAndAfter with TestHelperLogging with TestHelperMatchers {
  type FixtureParam = Map[String, Any]

  override def withFixture(test: OneArgTest) {
    DependencyInjection.get.foreach(_ => DependencyInjection.clear)
    DependencyInjection.set(defaultConfig(test.configMap))
    withLogging(test.configMap) {
      test(test.configMap)
    }
  }

  test("server") {
    config =>
      InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory())
      val threadPool = Executors.newCachedThreadPool()
      val factory = new NioDatagramChannelFactory(threadPool)
      val bootstrap = new ConnectionlessBootstrap(factory)
      bootstrap.setOption("reuseAddress", true)
      bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
        def getPipeline(): ChannelPipeline = {
          val pipeline = Channels.pipeline()
          pipeline.addFirst("execution-handler", new ExecutionHandler(
            new OrderedMemoryAwareThreadPoolExecutor(16, 1048576, 1048576)))
          pipeline.addLast("RUDP frame handler", new RUDPFrameHandler)
          pipeline.addLast("RUDP data handler", new RUDPDataHandler)
          pipeline.addLast("handler", new ServerChannelHandler)
          pipeline
        }
      })
      val channel = bootstrap.bind(RUDPTest.address)
      log.debug("start server")
      Thread.sleep(5000)
      log.debug("stop server")
      channel.close().awaitUninterruptibly()
      bootstrap.releaseExternalResources()
  }
  class ServerChannelHandler extends SimpleChannelHandler {
    override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) = e.getMessage() match {
      case message: String =>
        log.debug("message " + e.getMessage())
      case message =>
      // Note: in theory, we can't get here, because the
      // codec would have thrown an exception beforehand
      //log.fatal("Received unknown message object: " + message.getClass())
    }
    override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
      log.error("Server exception: " + e.getCause(), e.getCause())
    }
  }
}

object RUDPTest {
  val address = new InetSocketAddress("127.0.0.1", 21212)
  val data = "a" * 500 + "b" * 500 + "c" * 500 + "d" * 499 + ">"
}
