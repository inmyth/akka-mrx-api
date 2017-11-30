package exchange.mr.akka.api.flow

import scala.util.Try
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods.GET
import akka.http.scaladsl.model.{ HttpResponse, HttpRequest, StatusCodes, Uri }
import akka.http.scaladsl.model.ws.{ Message, TextMessage, UpgradeToWebSocket, WebSocketRequest }
import akka.stream.scaladsl.{ Flow, Source }
import play.api.libs.json.{ Json, JsValue }
import akka.http.scaladsl.model.Uri.apply
import play.api.libs.json.JsLookupResult.jsLookupResultToJsLookup
import play.api.libs.json.JsValue.jsValueToJsLookup
import play.api.libs.json.Json.toJsFieldJsValueWrapper
import scala.collection.Seq
import redis.clients.jedis.Jedis
import play.api.libs.json.JsObject

object ToMrf extends App {
  
  implicit val system = akka.actor.ActorSystem("ToMrf-system")
  implicit val materializer = akka.stream.ActorMaterializer()
  import system.dispatcher
  
  lazy val jedis =  new Jedis("54.205.99.208",6380)
  def isEmpty(x: String) = Option(x).forall(_.isEmpty)
  
  def isValidRequest(request: JsValue): Boolean = {
    val isSubmit = Try((request \ "command").as[String] == "submit").getOrElse(false)
    val isSecret = Try((request \ "secret").as[String].trim() != "").getOrElse(false)  
    isSubmit  || isSecret
  }
  
  def getSecret(apiKey: String): Option[String] = { 
    val secret = jedis.get(apiKey)   
    if (secret == null || secret.isEmpty()){
      return None
    } 
    else {
      return Some(secret)
    }   
  }

  // Flows --
  val userFlow = Flow[Message]

  val messageToJsValueFlow = Flow[Message] collect {
    case message: TextMessage.Strict => message.textStream.map(Json.parse(_)) collect {
      case jsValue if (isValidRequest(jsValue)) => jsValue
      case _ => Json.obj("command" -> "")
    } recover {
      case e: Exception =>
        //println(e)
        Json.obj("command" -> "")
    }
  }

  
  val jsValueSecretReplacedFlow = Flow[Source[JsValue, _]]
  .map(jst => {
    val updatedJsStream = jst.map(js => {
        val realSecret = getSecret((js \ "secret").as[String])
        val updatedJs = js.as[JsObject] + ("secret" -> Json.toJson(realSecret))
        println(updatedJs)
        updatedJs
    })
    updatedJsStream   
  })

  val jsValueToMessageFlow = Flow[Source[JsValue, _]] map { jsValueStream =>
    TextMessage.Streamed(jsValueStream.map(msg => { //TODO Filter "XRP" out
      Json.stringify(msg) })
    )}

  def rippledFlow = Http().webSocketClientFlow(WebSocketRequest("wss://rippled.mr.exchange"))

  def handlerFlow =
    userFlow via
    messageToJsValueFlow via
    jsValueSecretReplacedFlow via
    jsValueToMessageFlow via
    rippledFlow

  // Start listing --
  val binding = Http().bindAndHandleSync({
    case request@HttpRequest(GET, Uri.Path("/socket"), _, _, _) =>
      request
        .header[UpgradeToWebSocket]
        .map(_.handleMessages(handlerFlow))
        .getOrElse(HttpResponse(StatusCodes.BadRequest))
    case _ => HttpResponse(StatusCodes.NotFound)
  }, "localhost", 8080)

  // Waiting --
  println(s"Press return to shutdown")
  scala.io.StdIn.readLine()
  binding.flatMap(_.unbind()).andThen({ case _ => system.terminate() })
}