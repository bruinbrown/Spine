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
                let url = actorUrl fieldid nodeid
                let node = Spine.ActorSystem.System.ActorSelection(actorUrl fieldid nodeid)
                let! actor = node.ResolveOne(TimeSpan.FromSeconds(10.)) |> Async.AwaitTask
                printfn "%A" (actor.IsNobody())
                let streamreader = new System.IO.StreamReader(self.Request.Body)
                let! body = streamreader.ReadToEndAsync() |> Async.AwaitTask
                let sensors = Newtonsoft.Json.JsonConvert.DeserializeObject<NodeSensors>(body)
                let message = NodeMessage.SensorsAvailable(sensors)
                node <! message
                let resp = new Response(StatusCode = HttpStatusCode.OK)
                return (resp :> obj)
        } |> Async.StartAsTask

    do
        self.Post.["/field/{fieldid}/{nodeid}/startup", true] <- nodes

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
            if self.Context.CurrentUser = null then return (new Response(StatusCode = HttpStatusCode.Unauthorized) :> obj)
            else
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
            if self.Context.CurrentUser = null then return (new Response(StatusCode = HttpStatusCode.Unauthorized) :> obj)
            else
                let user = self.Context.CurrentUser.UserName
                let args = args :?> DynamicDictionary
                let fieldid = args.["fieldid"].ToString()
                let query = sprintf "SELECT COUNT(*) FROM fieldstate WHERE users CONTAINS '%s' AND fieldid = '%s'" self.Context.CurrentUser.UserName fieldid
                let! res = Spine.Configuration.Session.ExecuteAsync(new Cassandra.SimpleStatement(query)) |> Async.AwaitTask
                let count = res |> Seq.head |> fun t -> t.GetValue<int64>("count")
                if count = 0L then return (new Response(StatusCode = HttpStatusCode.Unauthorized) :> obj)
                else
                    let nodeid = args.["nodeid"].ToString()
                    let field = Spine.ActorSystem.System.ActorSelection(sprintf "akka://sensorsaurus/user/%s" fieldid)
                    let parameters = self.Bind<NodeCreationParameters>()
                    let msg = AddNode(Location(parameters.Latitude, parameters.Longitude), nodeid)
                    field <! msg
                    return (new Response(StatusCode = HttpStatusCode.OK) :> obj)
        } |> Async.StartAsTask

    do self.Post.["/field/{fieldid}", true] <- createField
    do self.Post.["/field/{fieldid}/{nodeid}", true] <- createNode

type NodeCreationService () as self =
    inherit NancyModule ()

    let createNode = fun(args:obj) (token:CancellationToken) ->
        async {
            return (new Response(StatusCode = HttpStatusCode.OK) :> obj)
        } |> Async.StartAsTask

    do
        self.Get.["/test", true] <- createNode

[<CLIMutable>]
type LoginModel =
    { Username : string
      Password : string }

[<CLIMutable>]
type LoginResponseModel =
    { ApiKey : string }

[<CLIMutable>]
type RegisterModel =
    { Username : string
      Email : string
      Password : string }

type AuthenticationService () as self =
    inherit NancyModule ()

    let rnd = Random ()
    let md5 = System.Security.Cryptography.MD5CryptoServiceProvider.Create()

    let rec generate_hash cnt hash =
        if cnt = 0 then System.Convert.ToBase64String(hash)
        else
            let new_bytes = md5.ComputeHash(hash)
            generate_hash (cnt-1) new_bytes

    let hashPassword (password:string) cycles =
        let bytes = System.Text.Encoding.UTF8.GetBytes(password)
        generate_hash cycles bytes

    let hashNewPassword (password:string) = 
        let cycles = rnd.Next(200)
        let bytes = System.Text.Encoding.UTF8.GetBytes (password)
        generate_hash cycles bytes, cycles
         

    let logIn = fun (args:obj) (token:CancellationToken) ->
        async {
            let lm = self.Bind<LoginModel>()
            let query = sprintf "SELECT hash, cycles FROM userregistrations WHERE username = '%s'" lm.Username
            let! resp = Spine.Configuration.Session.ExecuteAsync(Cassandra.SimpleStatement(query)) |> Async.AwaitTask
            let resp = resp |> Seq.cache
            if Seq.length resp = 0 then return (new Response(StatusCode = HttpStatusCode.Unauthorized) :> obj)
            else
                let data = resp |> Seq.head
                let correct_hash = data.GetValue<string>("hash")
                let cycles = data.GetValue<int>("cycles")
                let generated_hash = hashPassword lm.Password cycles
                if generated_hash = correct_hash then
                    let payload = [("sub", lm.Username)] |> dict
                    let token = JWT.JsonWebToken.Encode(payload, "supersecretkey", JWT.JwtHashAlgorithm.HS512)
                    return (self.Response.AsJson({ ApiKey = token }) :> obj)
                else
                    let response = new Response(StatusCode = HttpStatusCode.Unauthorized)
                    return (response :> obj)
        } |> Async.StartAsTask

    let register = fun (args:obj) (token:CancellationToken) ->
        async {
            let rm = self.Bind<RegisterModel>()
            let hash, cycles = hashNewPassword rm.Password
            let query = sprintf "INSERT INTO userregistrations (username, email, hash, cycles) VALUES ('%s', '%s', '%s', %i) IF NOT EXISTS" rm.Username rm.Email hash cycles
            let! resp = Spine.Configuration.Session.ExecuteAsync(Cassandra.SimpleStatement(query)) |> Async.AwaitTask
            let applied = resp |> Seq.head |> fun t -> t.GetValue<bool>("[applied]")
            if applied then
                let payload = [("sub", rm.Username)] |> dict
                let token = JWT.JsonWebToken.Encode(payload, "supersecretkey", JWT.JwtHashAlgorithm.HS512)
                return (self.Response.AsJson({ ApiKey = token }) :> obj)
            else return (new Response(StatusCode = HttpStatusCode.Conflict) :> obj)
        } |> Async.StartAsTask

    do self.Post.["/account/login", true] <- logIn
    do self.Post.["/account/register", true] <- register

type TimeService () as this =
    inherit NancyModule ()

    let retrieveTime = fun (args:obj) (token:CancellationToken) ->
        async {
            let data = args :?> DynamicDictionary
            let dt = DateTime.UtcNow |> Spine.Handy.ToUnixTimestamp
            let dt = dt.ToString()
            return this.Response.AsText(dt, "text/html") :> obj
        } |> Async.StartAsTask

    do
        this.Get.["/time", true] <- retrieveTime

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

[<CLIMutable>]
type NodeMetadataDto =
    { NodeId : string
      Latitude : float32
      Longitude : float32 }

type GetFieldService () as this =
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
                let query = sprintf "SELECT COUNT(*) FROM fieldstate WHERE users CONTAINS '%s' AND fieldid = '%s'" this.Context.CurrentUser.UserName fieldid
                let! res = Spine.Configuration.Session.ExecuteAsync(new Cassandra.SimpleStatement(query)) |> Async.AwaitTask
                let count = res |> Seq.head |> fun t -> t.GetValue<int64>("count")
                if count = 0L then return (new Response(StatusCode = HttpStatusCode.Unauthorized) :> obj)
                else
                    let nodeQuery = sprintf "SELECT * FROM nodestate WHERE fieldid ='%s'" fieldid
                    let! nodes = Spine.Configuration.Session.ExecuteAsync(Cassandra.SimpleStatement(nodeQuery)) |> Async.AwaitTask

                    let nodesDto = nodes
                                   |> Seq.map (fun t -> let nodeid = t.GetValue<string>("nodeid")
                                                        let latitude = t.GetValue<float32>("latitude")
                                                        let longitude = t.GetValue<float32>("longitude")
                                                        { NodeId = nodeid; Latitude = latitude; Longitude = longitude })
                                               

                    return (this.Response.AsJson(nodesDto) :> obj)
        } |> Async.StartAsTask

    do this.Get.["/field", true] <- retrieveFields
    do this.Get.["/field/{fieldid}", true] <- retrieveFieldData

open FSharp.Data

type InfluxSensors = FSharp.Data.JsonProvider<Sample = """[{"name":"testfield.testnode.testsensor","columns":["time","sequence_number","value","sensortype"],"points":[[1417696460200,8770001,3.2,"temp"]]}]""">
type InfluxSensorTypes = JsonProvider<Sample = """[{"name":"testfield.testnode.testsensor","columns":["time","sequence_number","value","sensortype"],"points":[[1417696460200,8770001,3.2,"temp"]]}]""">
type InfluxStatsData = JsonProvider<Sample = """[{"name":"testfield.testnode.testsensor","columns":["time","mean","max","latest"],"points":[[0,5888.887999999996,10114,886]]}]""">

type Point = { Time : int64; Value : float }

type SensorBrief = { Name : string; Points : Point seq }

type SensorReading = { Name : string; SensorType : string; Latitude : float32; Longitude : float32; Points : Point seq; Max : float32; Mean : float32; Latest : float32 }

type SensorDataService () as this =
    inherit NancyModule ()

    let retrieveSensorData = fun (args:obj) (token:CancellationToken) ->
        async {
            if this.Context.CurrentUser = null then return (new Response(StatusCode = HttpStatusCode.Unauthorized) :> obj)
            else
                let args = args :?> DynamicDictionary
                let fieldid = args.["fieldid"].ToString()
                let query = sprintf "SELECT COUNT(*) FROM fieldstate WHERE users CONTAINS '%s' AND fieldid = '%s'" this.Context.CurrentUser.UserName fieldid
                let! res = Spine.Configuration.Session.ExecuteAsync(new Cassandra.SimpleStatement(query)) |> Async.AwaitTask
                let count = res |> Seq.head |> fun t -> t.GetValue<int64>("count")
                if count = 0L then return (new Response(StatusCode = HttpStatusCode.Unauthorized) :> obj)
                else
                    let nodeid = args.["nodeid"].ToString()
                    let sensorids = args.["sensorid"].ToString().Split(';') |> Array.map (sprintf "'%s'") |> String.concat ","
                    let sensorid = args.["sensorid"].ToString().Split(';') |> Array.toList
                    let starttime = if args.ContainsKey "starttimestamp" then Some(args.["starttimestamp"].ToString()) else None
                    let endtime = if args.ContainsKey "endtimestamp" then Some(args.["endtimestamp"].ToString()) else None

                    let cql = sprintf "SELECT * FROM sensorstate WHERE fieldid = '%s' and nodeid = '%s' and sensorid in (%s)" fieldid nodeid sensorids
                    let! cassandra = Spine.Configuration.Session.ExecuteAsync(Cassandra.SimpleStatement(cql)) |> Async.AwaitTask

                    let sensors = cassandra
                                  |> Seq.map (fun t -> let fieldid = t.GetValue<string>("fieldid")
                                                       let nodeid = t.GetValue<string>("nodeid")
                                                       let sensorid = t.GetValue<string>("sensorid")
                                                       let sensorUrl = sprintf "%s.%s.%s" fieldid nodeid sensorid
                                                       let latitude = t.GetValue<float32>("latitude")
                                                       let longitude = t.GetValue<float32>("longitude")
                                                       let sensorType = t.GetValue<string>("sensortype")
                                                       sensorUrl, (sensorType, latitude, longitude))
                                  |> Map.ofSeq

                    let! executed = InfluxDB.SelectReadingsForSensors fieldid nodeid sensorid starttime endtime
                    let! statsData = InfluxDB.SelectStatsDataForAGivenSequenceOfSensors fieldid nodeid sensorid.[0] 
                    let parsedStats = InfluxStatsData.Parse(statsData)
                    let statsDict = parsedStats
                                    |> Seq.map (fun t -> let meanColumn = t.Columns |> Array.findIndex (fun s -> s = "mean")
                                                         let maxColumn = t.Columns |> Array.findIndex (fun s -> s = "max")
                                                         let latestColumn = t.Columns |> Array.findIndex (fun s -> s = "latest")
                                                         let mean = t.Points.[0].[meanColumn] |> float32
                                                         let max = t.Points.[0].[maxColumn] |> float32
                                                         let latest = t.Points.[0].[latestColumn] |> float32
                                                         t.Name, (mean, max, latest))
                                    |> dict
                    let json = executed
                               |> Option.map (fun t -> let reader = new System.IO.StringReader(t)
                                                       let parsed = InfluxSensors.Load(reader)
                                                       parsed
                                                       |> Seq.map (fun t -> let timeColumn = t.Columns |> Array.findIndex (fun s -> s = "time")
                                                                            let valueColumn = t.Columns |> Array.findIndex (fun t -> t = "value")
                                                                            let points = t.Points
                                                                                         |> Seq.map (fun s -> let array = s.JsonValue.AsArray ()
                                                                                                              let time = array.[timeColumn].AsInteger64()
                                                                                                              let value = array.[valueColumn].AsFloat()
                                                                                                              { Time = time; Value = value })
                                                                            let (sensorType, latitude, longitude) = sensors.[t.Name]
                                                                            let mean, max, latest = statsDict.[t.Name]
                                                                            { Name = t.Name; SensorType = sensorType; Latitude = latitude; Longitude = longitude; Points = points; Mean = mean; Max = max; Latest = latest }))
                       
                    match json with
                    | Some x -> return (this.Response.AsJson(x) :> obj)
                    | None -> return (new Response(StatusCode = HttpStatusCode.NotFound) :> obj)
        } |> Async.StartAsTask

    let sensorDataForGivenSensorType = fun (args:obj) (token:CancellationToken) ->
        async {
            if this.Context.CurrentUser = null then return (new Response(StatusCode = HttpStatusCode.Unauthorized) :> obj)
            else
                let args = args :?> DynamicDictionary
                let fieldid = args.["fieldid"].ToString()
                let query = sprintf "SELECT COUNT(*) FROM fieldstate WHERE users CONTAINS '%s' AND fieldid = '%s'" this.Context.CurrentUser.UserName fieldid
                let! res = Spine.Configuration.Session.ExecuteAsync(new Cassandra.SimpleStatement(query)) |> Async.AwaitTask
                let count = res |> Seq.head |> fun t -> t.GetValue<int64>("count")
                if count = 0L then return (new Response(StatusCode = HttpStatusCode.Unauthorized) :> obj)
                else
                    let sensorType = args.["sensortype"].ToString()
                    let! executed = InfluxDB.SelectReadingsForSensorType fieldid sensorType
                    let json = executed
                               |> Option.map (fun t -> let reader = new System.IO.StringReader(t)
                                                       let parsed = InfluxSensorTypes.Load(reader)
                                                       parsed
                                                       |> Seq.map (fun t -> let timeColumn = t.Columns |> Array.findIndex (fun s -> s = "time")
                                                                            let valueColumn = t.Columns |> Array.findIndex (fun t -> t = "value")
                                                                            let points = t.Points
                                                                                         |> Seq.map (fun s -> let array = s.JsonValue.AsArray ()
                                                                                                              let time = array.[timeColumn].AsInteger64()
                                                                                                              let value = array.[valueColumn].AsFloat()
                                                                                                              { Time = time; Value = value })
                                                                            let res : SensorBrief = { Name = t.Name; Points = points }
                                                                            res))
                    match json with
                    | Some x -> return (this.Response.AsJson(x) :> obj)
                    | None -> return (new Response(StatusCode = HttpStatusCode.NotFound) :> obj)
        } |> Async.StartAsTask

    do
        this.Get.["/field/{fieldid}/{nodeid}/{sensorid}/{starttimestamp?}/{endtimestamp?}", true] <- retrieveSensorData
        this.Get.["/field/{fieldid}/sensortype/{sensortype}", true] <- sensorDataForGivenSensorType

type ModifyNodeService () as this =
    inherit NancyModule ()

    let modifyNode = fun (args:obj) (token:CancellationToken) ->
        async {
            let args = args :?> DynamicDictionary
            let fieldid = args.["fieldid"].ToString()
            let query = sprintf "SELECT COUNT(*) FROM fieldstate WHERE users CONTAINS '%s' AND fieldid = '%s'" this.Context.CurrentUser.UserName fieldid
            let! res = Spine.Configuration.Session.ExecuteAsync(new Cassandra.SimpleStatement(query)) |> Async.AwaitTask
            let count = res |> Seq.head |> fun t -> t.GetValue<int64>("count")
            if count = 0L then return (new Response(StatusCode = HttpStatusCode.Unauthorized) :> obj)
            else
                let nodeid = args.["nodeid"].ToString()
                let location = this.Bind<NodeCreationParameters>() |> fun t -> Location(t.Latitude, t.Longitude)
                let nodeactor = Spine.ActorSystem.System.ActorSelection(sprintf "akka://sensorsaurus/user/%s/%s" fieldid nodeid)
                let! actor = nodeactor.ResolveOne(TimeSpan.FromSeconds(2.)) |> Async.AwaitTask
                nodeactor <! NodeMessage.SetLocation(location)
                return (new Response(StatusCode = HttpStatusCode.OK) :> obj)
        } |> Async.StartAsTask

    do
        this.Put.["/field/{fieldid}/{nodeid}", true] <- modifyNode

type InfluxNodeOverview = JsonProvider<Sample = """[{"name":"testfield.testnode.testsensor","columns":["time","mean","max","latest"],"points":[[0,3.466666666666667,4.8,3.2]]}]""">

type NodeOverviewDto =
    { Id : string
      Name : string
      Mean : float
      Max : float
      Latest : float }

type NodeOverviewService () as this =
    inherit NancyModule ()

    let retrieveRecentReadingsForNode = fun (args:obj) (token:CancellationToken) ->
        async {
            if this.Context.CurrentUser = null then return (new Response(StatusCode = HttpStatusCode.OK) :> obj)
            else
                let args = args :?> DynamicDictionary
                let fieldid = args.["fieldid"].ToString()
                let query = sprintf "SELECT COUNT(*) FROM fieldstate WHERE users CONTAINS '%s' AND fieldid = '%s'" this.Context.CurrentUser.UserName fieldid
                let! res = Spine.Configuration.Session.ExecuteAsync(new Cassandra.SimpleStatement(query)) |> Async.AwaitTask
                let count = res |> Seq.head |> fun t -> t.GetValue<int64>("count")
                if count = 0L then return (new Response(StatusCode = HttpStatusCode.OK) :> obj)
                else
                    let nodeid = args.["nodeid"].ToString()
                    let! executed = InfluxDB.SelectStatsDataForSensorsOnANode fieldid nodeid
                    let reader = new System.IO.StringReader(executed)
                    let parsed = InfluxNodeOverview.Load(reader)
                    let json = parsed
                               |> Seq.map (fun t -> let name = t.Name
                                                    let latestIndex = t.Columns |> Array.findIndex (fun s -> s = "latest")
                                                    let meanIndex = t.Columns |> Array.findIndex (fun s -> s = "mean")
                                                    let maxIndex = t.Columns |> Array.findIndex (fun s -> s = "max")
                                                    let latest = t.Points.[0].[latestIndex] |> float
                                                    let mean = t.Points.[0].[meanIndex] |> float
                                                    let max = t.Points.[0].[maxIndex] |> float
                                                    let id = name.Split('.').[2]
                                                    { Id = id; Name = name; Mean = mean; Latest = latest; Max = max}) |> Some

                    match json with
                    | Some x -> return (this.Response.AsJson(x) :> obj)
                    | None -> return (new Response(StatusCode = HttpStatusCode.NotFound) :> obj)
        } |> Async.StartAsTask

    do this.Get.["/field/{fieldid}/{nodeid}", true] <- retrieveRecentReadingsForNode

[<CLIMutable>]
type CreateConfigurationMessageParameter =
    { Polltime : int }

[<CLIMutable>]
type MessagePayload =
    { Polltime : int
      Address : string }

type ConfigurationMessageService (authentication:IAuthentication) as this =
    inherit NancyModule ()

    let createConfigurationMessage = fun (args:obj) (token:CancellationToken) ->
        async {
            if this.Context.CurrentUser = null then return (new Response(StatusCode = HttpStatusCode.Unauthorized) :> obj)
            else
                let args = args :?> DynamicDictionary
                let fieldid = args.["fieldid"].ToString()
                let query = sprintf "SELECT COUNT(*) FROM fieldstate WHERE users CONTAINS '%s' AND fieldid = '%s'" this.Context.CurrentUser.UserName fieldid
                let! res = Spine.Configuration.Session.ExecuteAsync(new Cassandra.SimpleStatement(query)) |> Async.AwaitTask
                let count = res |> Seq.head |> fun t -> t.GetValue<int64>("count")
                if count = 0L then return (new Response(StatusCode = HttpStatusCode.OK) :> obj)
                else
                    let pars = this.Bind<CreateConfigurationMessageParameter>()
                    let nodeid = if args.ContainsKey("nodeid") then Some(args.["nodeid"].ToString()) else None
                    let sensorid = if args.ContainsKey("sensorid") then Some(args.["sensorid"].ToString()) else None
                    let address = match nodeid, sensorid with
                                  | Some node, Some sensor -> sprintf "/%s/%s" node sensor
                                  | Some node, None -> sprintf "/%s" node
                                  | _ -> "/"
                    let payload = { Polltime = pars.Polltime; Address = address }
                                  |> Newtonsoft.Json.JsonConvert.SerializeObject
                    let query = sprintf "INSERT INTO configurationmessages (fieldid, payload,logged) VALUES ('%s', '%s', NOW())" fieldid payload
                    let! res = Spine.Configuration.Session.ExecuteAsync(Cassandra.SimpleStatement(query)) |> Async.AwaitTask
                    return (new Response(StatusCode = HttpStatusCode.OK) :> obj)
        } |> Async.StartAsTask

    let retrieveRecentConfigurationMessages = fun (args:obj) (token:CancellationToken) ->
        async {
            let apiKey = this.Request.Headers.Authorization.Split(' ')
            let args = args :?> DynamicDictionary
            if apiKey.Length <> 2 || apiKey.[0] <> "Key" then return (new Response(StatusCode = HttpStatusCode.Unauthorized) :> obj)
            else
                let fieldid = args.["fieldid"].ToString()
                let! isAuthed = authentication.AsyncIsAuthenticated fieldid apiKey.[1]
                if not isAuthed then return (new Response(StatusCode = HttpStatusCode.Unauthorized) :> obj)
                else
                    let since = args.["since"].ToString() |> Int32.Parse
                    let query = sprintf "SELECT payload FROM configurationmessages WHERE fieldid = '%s' AND logged > %i" fieldid since
                    let! res = Spine.Configuration.Session.ExecuteAsync(Cassandra.SimpleStatement(query)) |> Async.AwaitTask
                    let responseContent = res
                                          |> Seq.map (fun t -> t.GetValue<string>("payload"))
                                          |> String.concat ", "
                                          |> sprintf "[%s]"
                    return (this.Response.AsText(responseContent, "application/json") :> obj)
        } |> Async.StartAsTask

    do
        this.Get.["/field/{fieldid}/configuration/{nodeid?}/{sensorid?}", true] <- createConfigurationMessage
        this.Get.["/configuration/{fieldid}/{since}", true] <- retrieveRecentConfigurationMessages

type NodeDeletionService () as this =
    inherit NancyModule ()

    let deleteNode = fun (args:obj) (token:CancellationToken) ->
        async {
            if this.Context.CurrentUser = null then return (new Response(StatusCode = HttpStatusCode.Unauthorized) :> obj)
            else
                let args = args :?> DynamicDictionary
                let fieldid = args.["fieldid"].ToString()
                let query = sprintf "SELECT COUNT(*) FROM fieldstate WHERE users CONTAINS '%s' AND fieldid = '%s'" this.Context.CurrentUser.UserName fieldid
                let! res = Spine.Configuration.Session.ExecuteAsync(new Cassandra.SimpleStatement(query)) |> Async.AwaitTask
                let count = res |> Seq.head |> fun t -> t.GetValue<int64>("count")
                if count = 0L then return (new Response(StatusCode = HttpStatusCode.Unauthorized) :> obj)
                else
                    let nodeid = args.["nodeid"].ToString ()
                    let nodeAddress = sprintf "akka://sensorsaurus/user/field/%s/%s" fieldid nodeid
                    let node = Spine.ActorSystem.System.ActorSelection(nodeAddress)
                    let! actor = node.ResolveOne(TimeSpan.FromSeconds(4.)) |> Async.AwaitTask
                    let! success = actor.GracefulStop(TimeSpan.FromSeconds(5.)) |> Async.AwaitTask
                    do! InfluxDB.DeleteNodeSensors fieldid nodeid
                    let cql1 = sprintf "DELETE * FROM nodestate WHERE fieldid = '%s' AND nodeid = '%s'" fieldid nodeid
                    let cql2 = sprintf "DELETE * FROM sensorstate WHERE fieldid = '%s' AND nodeid = '%s'" fieldid nodeid
                    do! Spine.Configuration.Session.ExecuteAsync(Cassandra.SimpleStatement(cql1)) |> Async.AwaitTask |> Async.Ignore
                    do! Spine.Configuration.Session.ExecuteAsync(Cassandra.SimpleStatement(cql2)) |> Async.AwaitTask |> Async.Ignore
                    return (new Response(StatusCode = HttpStatusCode.OK) :> obj)
        } |> Async.StartAsTask

    do
        this.Delete.["/field/{fieldid}/{nodeid}", true] <- deleteNode