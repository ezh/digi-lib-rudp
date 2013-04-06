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

import scala.util.DynamicVariable

import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.channel.Channel
import org.jboss.netty.channel.ChannelDownstreamHandler
import org.jboss.netty.channel.ChannelEvent
import org.jboss.netty.channel.ChannelHandlerContext
import org.jboss.netty.channel.Channels
import org.jboss.netty.channel.MessageEvent
import org.jboss.netty.handler.codec.frame.FrameDecoder
import org.slf4j.LoggerFactory

class RUDPFrameHandler(val chunkLengthLimit: Int = RUDP.chunkLengthLimit) extends FrameDecoder with ChannelDownstreamHandler {
  val log = LoggerFactory.getLogger(this.getClass())
  /**
   * chunk header + footer
   * Header[8 bytes ID, 2 bytes POSITION, 2 bytes SEQUENCE LENGTH, 2 bytes DATA LENGTH , 4 bytes CRC32]
   * Footer[8 bytes pattern ]
   */
  val chunkMinimumLength = 26
  /** maximum packet size */
  val frameLengthLimit = chunkLengthLimit + chunkMinimumLength
  /** maximum buffer length */
  // there are three packets because maximum is:
  // 1st incomplete packet (frameLengthLimit - 1 byte)
  // 2nd full packet
  // 3rd incomplete packet (frameLengthLimit - 1 byte)
  val frameBufferLimit = frameLengthLimit * 3
  /** frame delimiter */
  val frameDelimiter = RUDP.chunkFooterBuffer
  /**
   * thread local bind of MessageEvent to codeflow
   * even the best framework have a piece of doggy code
   */
  val frameDecoderEvent = new DynamicVariable[MessageEvent](null)

  override def messageReceived(ctx: ChannelHandlerContext, evt: MessageEvent): Unit =
    frameDecoderEvent.withValue(evt) { super.messageReceived(ctx, evt) }
  /**
   * Unwrap RUDPDataHandler.Packet and send it forward
   */
  def handleDownstream(ctx: ChannelHandlerContext, evt: ChannelEvent) = evt match {
    case evt: MessageEvent =>
      evt.getMessage() match {
        case RUDPDataHandler.Packet(data) =>
          // unwrap and send forward
          Channels.write(ctx, evt.getFuture(), data, evt.getRemoteAddress())
        case msg =>
          // send forward
          ctx.sendDownstream(evt)
      }
    case evt =>
      ctx.sendDownstream(evt)
  }

  /**
   * Decode incoming buffer
   */
  override def decode(ctx: ChannelHandlerContext, channel: Channel, buffer: ChannelBuffer): Object = {
    if (buffer.readableBytes() < chunkMinimumLength) {
      null // too small
    } else {
      if (buffer.readableBytes() > frameBufferLimit)
        buffer.skipBytes(buffer.readableBytes() - frameBufferLimit)
      extractFrame(buffer) match {
        case Some((skipBytes, frame)) =>
          buffer.skipBytes(skipBytes)
          frame
        case None =>
          null
      }
    }
  }
  /**
   * Extract a frame of the specified buffer.
   */
  protected def extractFrame(buffer: ChannelBuffer): Option[(Int, RUDPFrameHandler.Packet)] = {
    readFrame(buffer, 0) orElse {
      var delimiterIndex = indexOf(buffer, frameDelimiter)
      while (delimiterIndex != -1) {
        readFrame(buffer, 0) match {
          case result: Some[_] =>
            return result
          case None =>
            delimiterIndex = indexOf(buffer, frameDelimiter, delimiterIndex)
        }
      }
      None
    }
  }
  /**
   * Read a frame at specified position
   */
  protected def readFrame(buffer: ChannelBuffer, index: Int): Option[(Int, RUDPFrameHandler.Packet)] = {
    buffer.markReaderIndex()
    buffer.skipBytes(index)
    val id = buffer.readLong()
    val nRel = buffer.readUnsignedShort()
    val nTotal = buffer.readUnsignedShort()
    val length = buffer.readUnsignedShort()
    val crc32 = buffer.readInt()
    if (buffer.readableBytes() < length + 8) {
      log.trace("skip frame #%d[%d] with length %d: tail shorter than data + footer".format(id, nRel, length))
      // tail shorter than data + footer
      buffer.resetReaderIndex()
      return None
    }
    val data = buffer.readSlice(length)
    val footer = buffer.readLong()
    if (footer != RUDP.chunkFooter) {
      log.trace("skip frame #%d[%d] with length %d: unknown footer 0x%X".format(id, nRel, length, footer))
      // unknown footer
      buffer.resetReaderIndex()
      return None
    }
    if (RUDPDataHandler.getCRC32(data) != crc32) {
      log.trace("skip frame #%d[%d] with length %d: CRC32 0x%X mismatch".format(id, nRel, length, crc32))
      // crc32 mismatch
      buffer.resetReaderIndex()
      val evt = frameDecoderEvent.value
      evt.getChannel().write(RUDPDataHandler.Packet(RUDP.buildResendPacket(id)), evt.getRemoteAddress())
      return None
    }
    val total = buffer.readerIndex()
    buffer.resetReaderIndex()
    Some(total - buffer.readerIndex(), RUDPFrameHandler.Packet(id, nRel, nTotal, data))
  }
  /**
   * Returns the number of bytes between the readerIndex of the haystack and
   * the first needle found in the haystack. -1 is returned if no needle is
   * found in the haystack.
   */
  protected def indexOf(haystack: ChannelBuffer, needle: ChannelBuffer): Int =
    indexOf(haystack, needle, 0)
  /**
   * Returns the number of bytes between the readerIndex of the haystack and
   * the first needle found in the haystack. -1 is returned if no needle is
   * found in the haystack.
   */
  protected def indexOf(haystack: ChannelBuffer, needle: ChannelBuffer, startIndex: Int): Int = {
    for (i <- haystack.readerIndex() + startIndex until haystack.writerIndex()) {
      var haystackIndex = i
      var needleIndex = 0
      while (needleIndex < needle.capacity() &&
        haystack.getByte(haystackIndex) == needle.getByte(needleIndex)) {
        haystackIndex += 1
        if (haystackIndex == haystack.writerIndex() &&
          needleIndex != needle.capacity() - 1) {
          return -1
        }
      }
      if (needleIndex == needle.capacity()) {
        // Found the needle from the haystack!
        return i - haystack.readerIndex()
      }
    }
    -1
  }
  /**
   * Override default newCumulationBuffer with frameBufferLimit allocator
   */
  override protected def newCumulationBuffer(ctx: ChannelHandlerContext, minimumCapacity: Int): ChannelBuffer = {
    val factory = ctx.getChannel.getConfig().getBufferFactory()
    factory.getBuffer(frameBufferLimit + frameLengthLimit)
  }
}

object RUDPFrameHandler {
  /** parsed RUDP chunk */
  case class Packet(nAbs: Long, nRel: Int, nTotal: Int, buffer: ChannelBuffer)
}
