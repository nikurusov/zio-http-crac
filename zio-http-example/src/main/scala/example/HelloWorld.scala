package example

import zio._
import zio.http._
import zio.http.netty.{ChannelType, NettyConfig}

object HelloWorld extends ZIOAppDefault {
  // Responds with plain text
  val homeRoute =
    Method.GET / Root -> handler(Response.text("Hello World!"))

  // Responds with JSON
  val jsonRoute =
    Method.GET / "json" -> handler(Response.json("""{"greetings": "Hello World!"}"""))

  // Create HTTP route
  val app = Routes(homeRoute, jsonRoute)

  val run = {
    val config           = Server.Config.default
      .port(8080)
    val nettyConfig      = NettyConfig.default.channelType(ChannelType.EPOLL)
    val configLayer      = ZLayer.succeed(config)
    val nettyConfigLayer = ZLayer.succeed(nettyConfig)

    app
      .serve[Any]
      .provide(configLayer, nettyConfigLayer, Server.customized)
  }
}
