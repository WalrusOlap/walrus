package org.pcg.walrus.common.util

import java.net.{InetAddress, NetworkInterface, Inet4Address, ServerSocket, SocketException}

import scala.collection.JavaConverters._

/**
  * Various utility methods for rpc
  */
object RpcUtil {

  /*
   * copyed from org.apache.spark.util.Utils.scala
   */
  def findLocalInetAddress(): InetAddress = {
    var address = InetAddress.getLocalHost
    if (address.isLoopbackAddress) {
      val activeNetworkIFs = NetworkInterface.getNetworkInterfaces.asScala.toSeq
      val reOrderedNetworkIFs = activeNetworkIFs.reverse

      for (ni <- reOrderedNetworkIFs) {
        val addresses = ni.getInetAddresses.asScala
          .filterNot(addr => addr.isLinkLocalAddress || addr.isLoopbackAddress).toSeq
        if (addresses.nonEmpty) {
          val addr = addresses.find(_.isInstanceOf[Inet4Address]).getOrElse(addresses.head)
          // because of Inet6Address.toHostName may add interface at the end if it knows about it
          address = InetAddress.getByAddress(addr.getAddress)
        }
      }
    }
    address
  }

  /**
    * port should be between 1024 and 65535.
    */
  def availablePort(maxTryTime: Int = 10): Int = {
    for(x <- 1 to maxTryTime) {
      val port = new scala.util.Random().nextInt(65535 - 1024) + 1024
      try {
        val s = new ServerSocket(port)
        s.close()
        return port
      } catch {
        case e: SocketException => // do nothing
      }
    }
    throw new Exception("no available port found")
  }
}