namespace Spine.Controllers

open Akka.Actor
open Spine.Model
open Akka.FSharp
open Spine.ApiConfiguration
open Nancy
open System.Threading.Tasks
open System.Threading
open System
open Nancy.ModelBinding
open Spine.Actors

type PostDataService (authentication:IAuthentication) as self =
    inherit NancyModule ()

    let actorUrl = sprintf "akka://sensorsaurus/user/%s/%s/%s"

    let postData = fun (args:obj) (token:CancellationToken) ->
        async {
            let args = args :?> DynamicDictionary
            let fieldid = (args.["fieldid"]).ToString()
            let nodeid = (args.["nodeid"]).ToString()
            let sensorid = (args.["sensorid"]).ToString()
            let apiKey = self.Context.Request.Headers.Authorization.Split(' ').[1]
            let! isAuthenticated = authentication.AsyncIsAuthenticated fieldid apiKey
            if not isAuthenticated then 
                let response = new Response(StatusCode = HttpStatusCode.Unauthorized)
                return (response :> obj)
            else
                let actor = Spine.ActorSystem.System.ActorSelection(actorUrl fieldid nodeid sensorid)
                let reading = self.Bind<SensorReading>()
                let msg = SensorMessage.Measurement(reading)
                actor <! msg
                let resp = new Response(StatusCode = HttpStatusCode.OK)
                return ( resp :> obj)
        } |> Async.StartAsTask

    do 
        self.Post.["/field/{fieldid}/{nodeid}/{sensorid}", true] <- postData

type FieldStartupService (authentication:IAuthentication) as self =
    inherit NancyModule ()

    let actorUrl = sprintf "akka://sensorsaurus/user/%s/%s"

    let nodes = fun (args:obj) (token:CancellationToken) ->
        async {
            let args = args :?> DynamicDictionary
            let fieldid = args.["fieldid"].ToString()
            let nodeid = args.["nodeid"].ToString()
            let apiKey = self.Context.Request.Headers.Authorization.Split(' ').[1]
            let! isAuthenticated = authentication.AsyncIsAuthenticated fieldid apiKey
            if not isAuthenticated then
                let response = new Response(StatusCode = HttpStatusCode.Unauthorized)
                return (response :> obj)
            else
                let node = Spine.ActorSystem.System.ActorSelection(actorUrl fieldid nodeid)
                let message = self.Bind<NodeSensors>() |> NodeMessage.SensorsAvailable
                node <! message
                let resp = new Response(StatusCode = HttpStatusCode.OK)
                return (resp :> obj)
        } |> Async.StartAsTask

    do
        self.Post.["/field/{fieldid}/{nodeid}", true] <- nodes

[<CLIMutable>]
type FieldCreationParameters =
    { Name : string }

[<CLIMutable>]
type NodeCreationParameters =
    { Latitude : float32
      Longitude : float32 }

type FieldCreationService (authentication:IAuthentication) as self =
    inherit NancyModule ()

    let createField = fun (args:obj) (token:CancellationToken) ->
        async {
            let user = self.Context.CurrentUser.UserName
            let args = args :?> DynamicDictionary
            let fieldid = args.["fieldid"].ToString()
            let args = self.Bind<FieldCreationParameters>()
            let field = spawn Spine.ActorSystem.System fieldid FieldActor
            let query = sprintf "INSERT INTO fieldstate (fieldid, name, users) VALUES ('%s', '%s', ['%s'])" fieldid args.Name user
            Spine.Configuration.Session.Execute(query) |> ignore
            return (new Response(StatusCode = HttpStatusCode.OK) :> obj)
        } |> Async.StartAsTask

    let createNode = fun (args:obj) (token:CancellationToken) ->
        async {
            let user = self.Context.CurrentUser.UserName
            let args = args :?> DynamicDictionary
            let fieldid = args.["fieldid"].ToString()
            let nodeid = args.["nodeid"].ToString()
            let field = Spine.ActorSystem.System.ActorSelection(sprintf "akka://sensorsaurus/user/%s" fieldid)
            let parameters = self.Bind<NodeCreationParameters>()
            let msg = AddNode(Location(parameters.Latitude, parameters.Longitude), nodeid)
            field <! msg
            return (new Response(StatusCode = HttpStatusCode.OK) :> obj)
        } |> Async.StartAsTask

    do self.Post.["/field/{fieldid}", true] <- createField
    do self.Post.["/field/{fieldid}/{nodeid}", true] <- createNode

[<CLIMutable>]
type LoginModel =
    { Username : string
      Password : string }

[<CLIMutable>]
type LoginResponseModel =
    { APIKey : string }

type AuthenticationService () as self =
    inherit NancyModule ()

    let logIn = fun (args:obj) (token:CancellationToken) ->
        async {
            let lm = self.Bind<LoginModel>()
            if lm.Username = "testaccount" && lm.Password = "password" then
                let payload = [("sub", lm.Username)] |> dict
                let token = JWT.JsonWebToken.Encode(payload, "supersecretkey", JWT.JwtHashAlgorithm.HS512)
                return (self.Response.AsJson({ APIKey = token }) :> obj)
            else
                let response = new Response(StatusCode = HttpStatusCode.Unauthorized)
                return (response :> obj)
        } |> Async.StartAsTask

    do self.Post.["/account/login", true] <- logIn

type TimeService () as this =
    inherit NancyModule ()

    do this.Get.["/time"] <- fun _ -> box <| Spine.Handy.ToUnixTimestamp (DateTime.UtcNow)

[<CLIMutable>]
type FieldDto =
    { FieldId : string
      FieldName : string
      Admins : string seq }

[<CLIMutable>]
type SensorMetadata =
    { SensorId : string
      SensorType : string
      LastContacted : DateTime
      Latitude : float32
      Longitude : float32 }

type FieldService () as this =
    inherit NancyModule ()

    let retrieveFields = fun (args:obj) (token:CancellationToken) ->
        async {
            if this.Context.CurrentUser = null then return (new Response(StatusCode = HttpStatusCode.Unauthorized) :> obj)
            else
                let username = this.Context.CurrentUser.UserName
                let query = sprintf "SELECT * FROM fieldstate WHERE users CONTAINS '%s'" username
                let! res = Spine.Configuration.Session.ExecuteAsync(new Cassandra.SimpleStatement(query)) |> Async.AwaitTask
                let fields = res
                             |> Seq.map (fun t -> let id = t.GetValue<string>("fieldid")
                                                  let name = t.GetValue<string>("name")
                                                  let users = t.GetValue<string seq>("users")
                                                  { FieldId = id; FieldName = name; Admins = users })
                return fields :> obj
        } |> Async.StartAsTask
       
    let retrieveFieldData = fun (args:obj) (token:CancellationToken) ->
        async {
            if this.Context.CurrentUser = null then return (new Response(StatusCode = HttpStatusCode.Unauthorized) :> obj)
            else
                let args = args :?> DynamicDictionary
                let fieldid = args.["fieldid"].ToString()
                let query = sprintf "SELECT COUNT(*) FROM fieldstate WHERE users CONTAINS '%s' AND fieldid = '%s'" fieldid this.Context.CurrentUser.UserName
                let! res = Spine.Configuration.Session.ExecuteAsync(new Cassandra.SimpleStatement(query)) |> Async.AwaitTask
                let count = res |> Seq.head |> fun t -> t.GetValue<int>("count")
                if count = 0 then return (new Response(StatusCode = HttpStatusCode.Unauthorized) :> obj)
                else
                    let query   = sprintf "SELECT * FROM sensorstate WHERE fieldid = '%s'" fieldid
                    let! res = Spine.Configuration.Session.ExecuteAsync(Cassandra.SimpleStatement(query)) |> Async.AwaitTask
                    let dtos = res
                               |> Seq.map (fun t -> let nodeid = t.GetValue<string>("nodeid")
                                                    let sensorid = t.GetValue<string>("nodeid")
                                                    let sensortype = t.GetValue<string>("sensortype")
                                                    let latitude = t.GetValue<float32>("latitude")
                                                    let longitude = t.GetValue<float32>("longitude")
                                                    let lastContacted = t.GetValue<DateTime>("lastcontacted")
                                                    nodeid, { SensorId = sensorid; SensorType = sensortype; Latitude = latitude; Longitude = longitude; LastContacted = lastContacted })
                               |> Seq.groupBy fst
                               |> Seq.map (fun (k,v) -> k, v |> Seq.map snd)

                    return (this.Response.AsJson(dtos) :> obj)
        } |> Async.StartAsTask

    do this.Get.["/field", true] <- retrieveFields
    do this.Get.["/field/{fieldid}", true] <- retrieveFieldData

type SensorDataService () as this =
    inherit NancyModule ()

    let retrieveSensorData = fun (args:obj) (token:CancellationToken) ->
        async {
            if this.Context.CurrentUser = null then return (new Response(StatusCode = HttpStatusCode.Unauthorized) :> obj)
            else
                let args = args :?> DynamicDictionary
                let fieldid = args.["fieldid"].ToString()
                let query = sprintf "SELECT COUNT(*) FROM fieldstate WHERE users CONTAINS '%s' AND fieldid = '%s'" fieldid this.Context.CurrentUser.UserName
                let! res = Spine.Configuration.Session.ExecuteAsync(new Cassandra.SimpleStatement(query)) |> Async.AwaitTask
                let count = res |> Seq.head |> fun t -> t.GetValue<int>("count")
                if count = 0 then return (new Response(StatusCode = HttpStatusCode.Unauthorized) :> obj)
                else
                    let nodeid = args.["nodeid"].ToString()
                    let sensorids = args.["sensorid"].ToString().Split(';') |> Array.map (sprintf "'%s'") |> String.concat ","
                    let starttime = if args.ContainsKey "starttimestamp" then Some(args.["starttimestamp"].ToString() |> int) else None
                    let endtime = if args.ContainsKey "endtimestamp" then Some(args.["endtimestamp"].ToString() |> int) else None
                    // TODO: Query InfluxDB here for data



                    return (new Response(StatusCode = HttpStatusCode.OK) :> obj)
        } |> Async.StartAsTask

    let sensorDataForGivenSensorType = fun (args:obj) (token:CancellationToken) ->
        async {
            return (new Response(StatusCode = HttpStatusCode.OK) :> obj)
        } |> Async.StartAsTask

    do
        this.Get.["/field/{fieldid}/{nodeid}/{sensorid}/{starttimestamp?}/{endtimestamp?}", true] <- retrieveSensorData
        this.Get.["/field/{fieldid}/sensors/{sensortype}", true] <- sensorDataForGivenSensorType