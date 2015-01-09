namespace Spine.ApiConfiguration

open Nancy
open Akka.Actor
open Nancy.Authentication.Stateless

type IAuthentication =
    abstract member AsyncIsAuthenticated : string -> string -> Async<bool>

type DefaultAuthentication () =
    interface IAuthentication with
        member this.AsyncIsAuthenticated fieldid apikey =
            async {
                if apikey = "1" then return true else return false
            }

type CustomNancyBootstrapper () =
    inherit DefaultNancyBootstrapper()

    override this.ApplicationStartup(container, pipelines) =

        let secretKey = "supersecretkey"

        let config = 
            new StatelessAuthenticationConfiguration(
                fun ctx -> let authHeader = ctx.Request.Headers.Authorization.Split(' ')
                           let typ, value = authHeader.[0], authHeader.[1]
                           if typ <> "API" then null
                           else
                               try
                                   let payload = JWT.JsonWebToken.DecodeToObject(value, secretKey)
                                   let payload = payload :?> System.Collections.Generic.IDictionary<string,obj>
                                   let username = payload.["sub"].ToString()
                                   { new Nancy.Security.IUserIdentity with
                                         member this.UserName with get () = username
                                         member this.Claims with get () = Seq.empty }
                               with
                               | :? JWT.SignatureVerificationException as sve -> null)

        StatelessAuthentication.Enable(pipelines, config)

        base.ApplicationStartup(container, pipelines)

    override this.RequestStartup(container, pipelines,context) =
        let handler = fun (ctx:NancyContext) -> ctx.Response.WithHeader("Access-Control-Allow-Origin", "*")
                                                            .WithHeader("Access-Control-Allow-Methods", "POST,GET")
                                                            .WithHeader("Access-Control-Allow-Headers", "Accept, Origin, Content-Type, Authorization") |> ignore
        pipelines.AfterRequest.AddItemToEndOfPipeline(handler)
        base.RequestStartup(container, pipelines, context)

    override this.ConfigureApplicationContainer(container) =

        container.Register<IAuthentication, DefaultAuthentication>() |> ignore

        base.ConfigureApplicationContainer(container)