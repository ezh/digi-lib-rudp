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

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.zip.CRC32

import scala.collection.mutable
import scala.ref.WeakReference

import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.channel.ChannelFuture
import org.jboss.netty.channel.ChannelFutureListener
import org.jboss.netty.channel.ChannelHandlerContext
import org.jboss.netty.channel.MessageEvent
import org.jboss.netty.channel.SimpleChannelHandler
import org.jboss.netty.util.HashedWheelTimer
import org.jboss.netty.util.Timeout
import org.jboss.netty.util.TimerTask
import org.slf4j.LoggerFactory

class RUDPDataHandler(val chunkLengthLimit: Int = RUDP.chunkLengthLimit) extends SimpleChannelHandler {
  val log = LoggerFactory.getLogger(this.getClass())
  /** id -> ChunkContainer */
  val outgoingChunkMap = new mutable.HashMap[Long, RUDPDataHandler.ChunkContainer] with mutable.SynchronizedMap[Long, RUDPDataHandler.ChunkContainer]
  /** event -> Seq(chunk id) */
  val outgoingEventMap = new mutable.WeakHashMap[MessageEvent, Seq[Long]] with mutable.SynchronizedMap[MessageEvent, Seq[Long]]
  /** id -> ChunkContainer */
  val incomingChunkMap = new mutable.HashMap[Long, Array[Option[RUDPFrameHandler.Packet]]] with mutable.SynchronizedMap[Long, Array[Option[RUDPFrameHandler.Packet]]]
  /** resend timer */
  val timer = new HashedWheelTimer
  /** resend delay */
  val timerDelay = 1000
  /** resend delay timeunit */
  val timerTimeUnit = TimeUnit.MILLISECONDS
  /** chunk time to live before it will mark as failed */
  val maximumTTL = 1000 * 60 // 1min
  /** resend maximum attempt */
  val maximumAttempt = 3
  /** chunk time to live before if will removed from handler */
  val garbageTTL = maximumTTL * 10
  /** global chunk packed id counter */
  val id = new AtomicLong(0)
  /** maximum chunks per message */
  val chunkNumLimit = 65535 // unsigned short

  override def messageReceived(ctx: ChannelHandlerContext, evt: MessageEvent) = evt.getMessage() match {
    case packet @ RUDPFrameHandler.Packet(RUDP.servicePacket, packetType, nTotal, buffer) =>
      try {
        RUDP.PacketType(packetType) match {
          case RUDP.PacketType.Reset =>
            log.info("reset connection")
            outgoingChunkMap.clear
            outgoingEventMap.clear
            incomingChunkMap.clear
          case RUDP.PacketType.Successful =>
            for (i <- 0 until nTotal) {
              val nAbs = buffer.readLong()
              outgoingChunkMap.get(nAbs) match {
                case Some(chunk) =>
                  chunk.payload match {
                    case Right(payload) =>
                    // skip - already delivered/failed
                    case Left(payload) =>
                      payload.setSuccess
                  }
                case None =>
                  log.warn("receive Successful package for unknown RUDP chunk #{}", nAbs)
              }
            }
          case RUDP.PacketType.Resend =>
            for (i <- 0 until nTotal) {
              val nAbs = buffer.readLong()
              outgoingChunkMap.get(nAbs) match {
                case Some(chunk) =>
                  chunk.payload match {
                    case Right(payload) =>
                    // skip - already delivered/failed
                    case Left(payload) =>
                      payload.write()
                  }
                case None =>
                  log.warn("receive Resend package for unknown RUDP chunk #{}", nAbs)
              }
            }
        }
      } catch {
        case e: NoSuchElementException =>
          log.warn("broken service packet received " + packet)
      }
    case packet @ RUDPFrameHandler.Packet(nAbs, nRel, nTotal, buffer) =>
      val nAbsStart = nAbs - (nRel - 1)
      incomingChunkMap.get(nAbsStart) match {
        case Some(arrayOfChunks) =>
          if (arrayOfChunks(nRel - 1).isEmpty) {
            log.trace("receive chunk #%d[%d/%d] with length %d".format(nAbs, nRel, nTotal, buffer.readableBytes()))
            arrayOfChunks(nRel - 1) = Some(packet)
            evt.getChannel().write(RUDPDataHandler.Packet(RUDP.buildAckPacket(nAbs)), evt.getRemoteAddress())
          }
        case None =>
          log.trace("receive chunk #%d[%d/%d] with length %d".format(nAbs, nRel, nTotal, buffer.readableBytes()))
          val array = Array.fill[Option[RUDPFrameHandler.Packet]](nTotal)(None)
          // chunks relative position starts from 1
          array(nRel - 1) = Some(packet)
          incomingChunkMap(nAbsStart) = array
          evt.getChannel().write(RUDPDataHandler.Packet(RUDP.buildAckPacket(nAbs)), evt.getRemoteAddress())
      }
    // join packet
    case message =>
  }
  override def writeRequested(ctx: ChannelHandlerContext, evt: MessageEvent) = evt.getMessage() match {
    case raw: ChannelBuffer =>
      log.trace("split message with size " + raw.readableBytes())
      // split raw message
      val totalBytes = raw.readableBytes()
      val totalChunks = (totalBytes / chunkLengthLimit) + 1
      assert(totalChunks < chunkNumLimit, "message too large")
      val chunkIDSeq = for (i <- 0 until totalChunks) yield {
        val chunkID = id.incrementAndGet() match {
          case RUDP.servicePacket => id.incrementAndGet()
          case id => id
        }
        val tail = chunkLengthLimit * (i + 1) - totalBytes
        val sliceSize = if (tail < 0) chunkLengthLimit else chunkLengthLimit - tail
        // chunks relative position starts from 1
        val buffer = RUDP.buildDataPacket(chunkID, i + 1, totalChunks, raw.readSlice(sliceSize))
        val pendingElement = RUDPDataHandler.Pending(chunkID, i + 1, totalChunks, buffer, evt, new WeakReference(this))
        outgoingChunkMap(chunkID) = RUDPDataHandler.ChunkContainer(Left(pendingElement))
        chunkID
      }
      outgoingEventMap(evt) = chunkIDSeq
      // send
      chunkIDSeq.foreach(id => outgoingChunkMap(id).payload.left.map(_.write))
    case msg =>
      super.writeRequested(ctx, evt)
  }
}

object RUDPDataHandler {
  val log = LoggerFactory.getLogger(this.getClass())

  /**
   * Calculate a crc32 checksum of the given buffer.
   *
   * @param buffer which checksum should be calculated
   * @return calculated checksum
   */
  def getCRC32(buffer: ChannelBuffer): Int = {
    val crc = new CRC32()
    for (bb <- buffer.toByteBuffers) {
      val b = new Array[Byte](bb.remaining())
      bb.get(b)
      crc.update(b)
    }
    crc.getValue().toInt & 0xffffffff
  }

  case class Pending(nAbs: Long, nRel: Int, nTotal: Int, buffer: ChannelBuffer, val evt: MessageEvent, val handler: WeakReference[RUDPDataHandler]) {
    @volatile var attempt = 0

    /**
     * write chunk to evt ChannelBuffer
     */
    def write(): Unit = synchronized {
      handler.get match {
        case Some(handler) =>
          val chunk = handler.outgoingChunkMap(nAbs)
          if (chunk.payload.isLeft) // we are still pending
            if (attempt >= handler.maximumAttempt) {
              val msg = "send chunk #%d[%d] with length %d failed: maximum attempts exceeded".format(nAbs, nRel, buffer.readableBytes(), attempt)
              setFailure(new Throwable(msg))
            } else if ((chunk.timestamp + handler.maximumTTL) < System.currentTimeMillis) {
              val msg = "send chunk #%d[%d] with length %d failed: TTL exceeded".format(nAbs, nRel, buffer.readableBytes(), attempt)
              setFailure(new Throwable(msg))
            } else {
              attempt += 1
              log.trace("send attempt %d chunk #%d[%d/%d] with length %d".format(attempt, nAbs, nRel, nTotal, buffer.readableBytes()))
              val future = evt.getChannel().write(RUDPDataHandler.Packet(buffer), evt.getRemoteAddress())
              future.addListener(new ChannelFutureListener() {
                def operationComplete(future: ChannelFuture) {
                  val tt = new TimerTask { def run(timeout: Timeout) = write() }
                  handler.timer.newTimeout(tt, handler.timerDelay, handler.timerTimeUnit)
                }
              })
            }
        case None =>
          val msg = "Handler is absent for " + this
          log.error(msg, new Throwable(msg))
      }
    }
    /**
     * set failure for this chunk
     */
    def setFailure(t: Throwable) = {
      handler.get match {
        case Some(handler) =>
          log.warn(t.getMessage())
          handler.outgoingChunkMap(nAbs) = ChunkContainer(Right(Some(t)), handler.outgoingChunkMap(nAbs).timestamp)
          updateResult()
        case None =>
          val msg = "Handler is absent for " + this
          log.error(msg, new Throwable(msg))
      }
    }
    /**
     * set success for this chunk
     */
    def setSuccess() = {
      handler.get match {
        case Some(handler) =>
          log.trace("chunk #%d[%d] with length %d delivered at %d attempt".format(nAbs, nRel, nTotal, buffer.readableBytes(), attempt))
          handler.outgoingChunkMap(nAbs) = ChunkContainer(Right(None), handler.outgoingChunkMap(nAbs).timestamp)
          updateResult()
        case None =>
          val msg = "Handler is absent for " + this
          log.error(msg, new Throwable(msg))
      }
    }
    /**
     * fire setFailure or setSuccess if all chunks are processed
     */
    protected def updateResult() = synchronized {
      handler.get match {
        case Some(handler) =>
          if (handler.outgoingEventMap(evt).forall { id =>
            handler.outgoingChunkMap.get(id) match {
              case Some(chunkContainer) =>
                chunkContainer.payload.isRight
              case None =>
                val msg = "chunk #%d is lost".format(id)
                log.error(msg, new Throwable(msg))
                false
            }
          }) {
            handler.outgoingEventMap(evt).find(id => handler.outgoingChunkMap.get(id).map(_.payload.right.get.isDefined) getOrElse true) match {
              case Some(id) =>
                // failure
                handler.outgoingChunkMap.get(id) match {
                  case Some(chunk) =>
                    evt.getFuture().setFailure(chunk.payload.right.get.get)
                  case None =>
                    val msg = "chunk #%d is lost".format(id)
                    evt.getFuture().setFailure(new Throwable(msg))
                }
              case None =>
                // success
                evt.getFuture().setSuccess()
            }
          }
        case None =>
          val msg = "Handler is absent for " + this
          log.error(msg, new Throwable(msg))
      }
    }
  }
  /**
   * payload: Left[Pending] if chunk is waiting
   *          Right[Option[Throwable] if chunk sent, where
   *            Right[None]            if delivery was successful
   *            Right[Some[Throwable]] if delivery was failed
   */
  case class ChunkContainer(val payload: Either[Pending, Option[Throwable]], val timestamp: Long = System.currentTimeMillis)
  /** raw RUDP chunk */
  case class Packet(data: ChannelBuffer)
}
