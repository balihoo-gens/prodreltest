package com.balihoo.fulfillment.dashboard

import javax.servlet.http.{HttpServletResponse, HttpServletRequest, HttpServlet}

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.ServletHolder
import org.eclipse.jetty.webapp.WebAppContext

class WorkerServlet extends HttpServlet {

  override protected def doGet(request:HttpServletRequest,
                               response:HttpServletResponse) = {


    response.setContentType("application/json")
    response.setStatus(HttpServletResponse.SC_OK)
    response.getWriter.println("""{ "message" : "HELLO THERE" }""")
  }

}

object dashboard {
  def main(args: Array[String]) {
    val port = 8080

    val server = new Server(port)
    val context = new WebAppContext()
    context setContextPath "/"
    context.setResourceBase("src/main/webapp")
    context.addServlet(new ServletHolder(new WorkerServlet), "/worker/*")
    context.setWelcomeFiles(Array[String]("index.html"))

    server.setHandler(context)

    server.start()
    server.join()
  }
}