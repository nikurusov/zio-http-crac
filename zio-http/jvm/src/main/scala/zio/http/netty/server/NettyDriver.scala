/*
 * Copyright 2021 - 2023 Sporta Technologies PVT LTD & the ZIO HTTP contributors.
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

package zio.http.netty.server

import java.lang.{Boolean => JBoolean}
import java.net.InetSocketAddress
import zio._
import zio.http.Driver.StartResult
import zio.http.netty._
import zio.http.netty.client.NettyClientDriver
import zio.http.{ClientDriver, Driver, Response, Routes, Server}
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel._
import io.netty.channel.epoll.EpollEventLoop
import io.netty.util.ResourceLeakDetector
import org.crac.Resource
import org.crac.Context

import java.util.concurrent.atomic.AtomicReference
import org.crac.Core

import scala.util.{Failure, Success, Try}

final case class NettyDriver(
  appRef: RoutesRef,
  channelFactory: ChannelFactory[ServerChannel],
  channelInitializer: ChannelInitializer[Channel],
  serverInboundHandler: ServerInboundHandler,
  eventLoopGroups: ServerEventLoopGroups,
  serverConfig: Server.Config,
  nettyConfig: NettyConfig,
) extends Driver
    with Resource { self =>

  private val activeListenersCloseHooks = scala.collection.mutable.ListBuffer.empty[() => ChannelFuture]
  private val scopeRef                  = new AtomicReference[zio.Scope](null)

  def start(implicit trace: Trace): RIO[Scope, StartResult] =
    for {
      thisScope <- ZIO.scope
      _ = scopeRef.set(thisScope)

      _   <- ZIO.attempt(Core.getGlobalContext().register(this))
      chf <- ZIO.attempt {
        new ServerBootstrap()
          .group(eventLoopGroups.boss, eventLoopGroups.worker)
          .channelFactory(channelFactory)
          .childHandler(channelInitializer)
          .option[Integer](ChannelOption.SO_BACKLOG, serverConfig.soBacklog)
          .childOption[JBoolean](ChannelOption.TCP_NODELAY, serverConfig.tcpNoDelay)
          .bind(serverConfig.address)
      }

      _       <- NettyFutureExecutor.scoped(chf)
      _       <- ZIO.succeed(ResourceLeakDetector.setLevel(nettyConfig.leakDetectionLevel.toNetty))
      channel <- ZIO.attempt(chf.channel())

      _ = activeListenersCloseHooks.addAll(
        List(() => { chf.cancel(true); chf.await() }, () => channel.close()),
      )

      port <- ZIO.attempt(channel.localAddress().asInstanceOf[InetSocketAddress].getPort)

      _ <- Scope.addFinalizer(
        NettyFutureExecutor.executed(channel.close()).ignoreLogged,
      )
    } yield StartResult(port, serverInboundHandler.inFlightRequests)

  def addApp[R](newApp: Routes[R, Response], env: ZEnvironment[R])(implicit trace: Trace): UIO[Unit] =
    ZIO.fiberId.map { fiberId =>
      var loop = true
      while (loop) {
        val oldAppAndRt     = appRef.get()
        val (oldApp, oldRt) = oldAppAndRt
        val updatedApp      = (oldApp ++ newApp).asInstanceOf[Routes[Any, Response]]
        val updatedEnv      = oldRt.environment.unionAll(env)
        // Update the fiberRefs with the new environment to avoid doing this every time we run / fork a fiber
        val updatedFibRefs  = oldRt.fiberRefs.updatedAs(fiberId)(FiberRef.currentEnvironment, updatedEnv)
        val updatedRt       = Runtime(updatedEnv, updatedFibRefs, oldRt.runtimeFlags)
        val updatedAppAndRt = (updatedApp, updatedRt)

        if (appRef.compareAndSet(oldAppAndRt, updatedAppAndRt)) loop = false
      }
      serverInboundHandler.refreshApp()
    }

  override def createClientDriver()(implicit trace: Trace): ZIO[Scope, Throwable, ClientDriver] =
    for {
      channelFactory <- ChannelFactories.Client.live.build
        .provideSomeEnvironment[Scope](_ ++ ZEnvironment[ChannelType.Config](nettyConfig))
      nettyRuntime   <- NettyRuntime.live.build
    } yield NettyClientDriver(channelFactory.get, eventLoopGroups.worker, nettyRuntime.get)

  override def toString: String = s"NettyDriver($serverConfig)"

  // CRaC support
  override def beforeCheckpoint(context: Context[_ <: Resource]): Unit = {
    eventLoopGroups.boss.shutdownGracefully().awaitUninterruptibly()
    eventLoopGroups.worker.shutdownGracefully().awaitUninterruptibly()
    activeListenersCloseHooks.foreach(fut => fut().awaitUninterruptibly())
    activeListenersCloseHooks.clear()
  }

  override def afterRestore(context: Context[_ <: Resource]): Unit = {
    val appScope = Option(scopeRef.get())
      .getOrElse(throw new RuntimeException("Scope is empty, can't restore app"))

    val bossEventLoopZIO =
      (ZLayer.succeed(nettyConfig.bossGroup) >+> EventLoopGroups.live).build
        .map(e => e.get[EventLoopGroup])

    val workerEventLoopZIO =
      (ZLayer.succeed(nettyConfig.bossGroup) >+> EventLoopGroups.live).build
        .map(e => e.get[EventLoopGroup])

    val newServer = for {
      bossEL   <- bossEventLoopZIO
      workerEL <- workerEventLoopZIO
      chf      <- ZIO.attempt {
        new ServerBootstrap()
          .group(bossEL, workerEL)
          .channelFactory(channelFactory)
          .childHandler(channelInitializer)
          .option[Integer](ChannelOption.SO_BACKLOG, serverConfig.soBacklog)
          .childOption[JBoolean](ChannelOption.TCP_NODELAY, serverConfig.tcpNoDelay)
          .bind(serverConfig.address)
      }
      _        <- NettyFutureExecutor.scoped(chf)
      _        <- ZIO.succeed(ResourceLeakDetector.setLevel(nettyConfig.leakDetectionLevel.toNetty))
      channel  <- ZIO.attempt(chf.channel())

      _ = activeListenersCloseHooks.addAll(
        List(() => { chf.cancel(true); chf.await() }, () => channel.close()),
      )
      _ <- Scope.addFinalizer(
        NettyFutureExecutor.executed(channel.close()).ignoreLogged,
      )
    } yield ()

    zio.Unsafe.unsafe { implicit u =>
      zio.Runtime.default.unsafe.run(newServer.provide(ZLayer.succeed(appScope)))
    }

    ()
  }
}

object NettyDriver {

  implicit val trace: Trace = Trace.empty

  val make: ZIO[
    RoutesRef
      & ChannelFactory[ServerChannel]
      & ChannelInitializer[Channel]
      & ServerEventLoopGroups
      & Server.Config
      & NettyConfig
      & ServerInboundHandler,
    Nothing,
    Driver,
  ] =
    for {
      app   <- ZIO.service[RoutesRef]
      cf    <- ZIO.service[ChannelFactory[ServerChannel]]
      cInit <- ZIO.service[ChannelInitializer[Channel]]
      elg   <- ZIO.service[ServerEventLoopGroups]
      sc    <- ZIO.service[Server.Config]
      nsc   <- ZIO.service[NettyConfig]
      sih   <- ZIO.service[ServerInboundHandler]
    } yield new NettyDriver(
      appRef = app,
      channelFactory = cf,
      channelInitializer = cInit,
      serverInboundHandler = sih,
      eventLoopGroups = elg,
      serverConfig = sc,
      nettyConfig = nsc,
    )

  val manual
    : ZLayer[ServerEventLoopGroups & ChannelFactory[ServerChannel] & Server.Config & NettyConfig, Nothing, Driver] = {
    implicit val trace: Trace = Trace.empty
    ZLayer.makeSome[ServerEventLoopGroups & ChannelFactory[ServerChannel] & Server.Config & NettyConfig, Driver](
      ZLayer(AppRef.empty),
      ServerChannelInitializer.layer,
      ServerInboundHandler.live,
      ZLayer(make),
    )
  }

  val customized: ZLayer[Server.Config & NettyConfig, Throwable, Driver] = {
    val serverChannelFactory: ZLayer[NettyConfig, Nothing, ChannelFactory[ServerChannel]] =
      ChannelFactories.Server.fromConfig
    val eventLoopGroup: ZLayer[NettyConfig, Nothing, ServerEventLoopGroups]               = ServerEventLoopGroups.live

    ZLayer.makeSome[Server.Config & NettyConfig, Driver](
      eventLoopGroup,
      serverChannelFactory,
      manual,
    )
  }

  val live: ZLayer[Server.Config, Throwable, Driver] =
    ZLayer.makeSome[Server.Config, Driver](
      ZLayer.succeed(NettyConfig.default),
      customized,
    )
}
