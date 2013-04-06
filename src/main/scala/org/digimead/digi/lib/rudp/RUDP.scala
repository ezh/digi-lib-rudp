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

import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers

object RUDP {
  /** Reserved id for service packet */
  val servicePacket = 7L
  /** chunk footer */
  val chunkFooter = 0x123456789abcdef0L
  /** chunk footer buffer */
  val chunkFooterBuffer = {
    val buffer = ChannelBuffers.buffer(8)
    buffer.writeLong(chunkFooter)
    buffer
  }
  /** maximum bytes per chunk */
  val chunkLengthLimit = 742 // 768(default maximum) - 18(header) - 8(footer)

  def buildDataPacket(nAbs: Long, nRel: Int, nTotal: Int, body: ChannelBuffer): ChannelBuffer = {
    assert(nRel > 0 && nRel < 65536, "Incorrect nRel value " + nRel)
    assert(nTotal > 0 && nTotal < 65536, "Incorrect nTotal value " + nTotal)
    assert(body.readableBytes() < 65536, "Body length too large " + body.readableBytes())
    val chunkHeaderBuffer = ChannelBuffers.buffer(18)
    chunkHeaderBuffer.writeLong(nAbs)
    chunkHeaderBuffer.writeShort(nRel)
    chunkHeaderBuffer.writeShort(nTotal)
    chunkHeaderBuffer.writeShort(body.readableBytes())
    val crc32 = RUDPDataHandler.getCRC32(body)
    chunkHeaderBuffer.writeInt(crc32)
    ChannelBuffers.wrappedBuffer(chunkHeaderBuffer, body, chunkFooterBuffer)
  }
  def buildAckPacket(nAbs: Long): ChannelBuffer = {
    val body = ChannelBuffers.buffer(8)
    body.writeLong(nAbs)
    val chunkHeaderBuffer = ChannelBuffers.buffer(18)
    chunkHeaderBuffer.writeLong(servicePacket)
    chunkHeaderBuffer.writeShort(PacketType.Successful.id)
    chunkHeaderBuffer.writeShort(1)
    chunkHeaderBuffer.writeShort(body.readableBytes())
    val crc32 = RUDPDataHandler.getCRC32(body)
    chunkHeaderBuffer.writeInt(crc32)
    ChannelBuffers.wrappedBuffer(chunkHeaderBuffer, body, chunkFooterBuffer)
  }
  def buildResendPacket(nAbs: Long): ChannelBuffer = {
    val body = ChannelBuffers.buffer(8)
    body.writeLong(nAbs)
    val chunkHeaderBuffer = ChannelBuffers.buffer(18)
    chunkHeaderBuffer.writeLong(servicePacket)
    chunkHeaderBuffer.writeShort(PacketType.Resend.id)
    chunkHeaderBuffer.writeShort(1)
    chunkHeaderBuffer.writeShort(body.readableBytes())
    val crc32 = RUDPDataHandler.getCRC32(body)
    chunkHeaderBuffer.writeInt(crc32)
    ChannelBuffers.wrappedBuffer(chunkHeaderBuffer, body, chunkFooterBuffer)
  }
  /** Service packet types */
  object PacketType extends Enumeration {
    val Reset, Successful, Resend = Value
  }
}
