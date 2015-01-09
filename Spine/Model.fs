namespace Spine.Model

open System
open Nancy.Security

[<CLIMutable>]
type SensorReading =
    { Timestamp : int32
      Value : float }

[<CLIMutable>]
type Sensor =
    { SensorId : string
      SensorType : string }

[<CLIMutable>]
type NodeSensors =
    { Sensors : Sensor seq }

type Location = Location of float32 * float32

type BatteryStatus = BatteryStatus of int

type FieldMessage =
    | AddNode of Location * string

type NodeMessage =
    | SetLocation of Location
    | SetBatteryStatus of BatteryStatus
    | SensorsAvailable of NodeSensors

type SensorMessage =
    | Measurement of SensorReading
    | SetLocation of Location

type SensorConfiguration =
    { Location : Location
      SensorType : string
      LastContacted : DateTime }

type NodeConfiguration =
    { BatteryStatus : BatteryStatus
      Location : Location}

type FieldConfiguration =
    { Name : string }

type CassandraWriteMessage =
    | StoreSensorState of FieldId:string * NodeId:string * SensorId:string * SensorConfiguration
    | StoreNodeState of FieldId:string * NodeId:string * NodeConfiguration

type InfluxDBWriteMessage =
    | StorePoint of Series:string * Timestamp:string * Value:string * FieldID:string * NodeID:string * SensorID:string * Location:Location 
    

namespace Spine

module Handy =

    let ToUnixTimestamp (dt:System.DateTime) =
        System.DateTime.UtcNow.Subtract(new System.DateTime(1970,1,1)).TotalSeconds |> int32

module Configuration =

    let InfluxDBUrl = "http://178.62.84.55:8086"
    let CassandraAddress = "178.62.85.104"
    let SensorsaurusKeyspace = "sensorsaurus"

    let Cluster = Cassandra.Cluster.Builder().AddContactPoint(CassandraAddress).Build()
    let Session = Cluster.Connect(SensorsaurusKeyspace)

    open System
    open Spine.Model

    let private DefaultNodeConfiguration = { Location = Location(0.f,0.f); BatteryStatus = BatteryStatus(100) }

    let LoadNodeStateFromCassandra fieldid nodeid =
        let statequery = sprintf "SELECT * FROM nodestate WHERE fieldid = '%s' and nodeid = '%s'" fieldid nodeid
        let resultsSet = Session.Execute(statequery) |> Seq.cache
        if resultsSet |> Seq.length <> 0 then
            let cfg = resultsSet |> Seq.head
            let latitude = cfg.GetValue<float32>("latitude")
            let longitude = cfg.GetValue<float32>("longitude")
            { Location = Location(latitude, longitude); BatteryStatus = BatteryStatus(100) }
        else
            DefaultNodeConfiguration

    let DefaultSensorConfiguration = { Location = Location(0.f, 0.f); LastContacted = DateTime.UtcNow; SensorType = "" }

    let LoadSensorStateFromCassandra fieldid nodeid sensorid =
        let statequery = sprintf """SELECT * FROM sensorstate where fieldid = '%s' and nodeid = '%s' and sensorid = '%s'""" fieldid nodeid sensorid
        let resultsSet = Session.Execute(statequery)
        if resultsSet.GetAvailableWithoutFetching() = 0 then DefaultSensorConfiguration
        else
            let head = resultsSet.GetRows() |> Seq.head
            let latitude = head.GetValue<float32>("latitude")
            let longitude = head.GetValue<float32>("longitude")
            let lastcontacted = if head.IsNull("lastcontacted") then DateTime.MinValue else head.GetValue<DateTime>("lastcontacted")
            let sensorType = head.GetValue<string>("sensortype")
            { Location = Location(latitude, longitude); LastContacted = lastcontacted; SensorType = sensorType }

namespace Spine

    module Actors =

        open Akka.FSharp
        open Akka
        open Spine.Model
        open Spine
        open Spine.Configuration
        open Newtonsoft.Json
        open HttpClient

        [<CLIMutable>]
        type InfluxDBPoints =
            { name : string
              columns : string list
              points : obj list list }

        let SensorActor sensorType (mailbox:Actor<SensorMessage>) =
            let sensorid = mailbox.Self.Path.Name
            let nodeid = mailbox.Self.Path.Parent.Name
            let fieldid = mailbox.Self.Path.Parent.Parent.Name
            let seriesName = sprintf "%s.%s.%s" fieldid nodeid sensorid

            let rec loop (config:SensorConfiguration) = actor {
                let! msg = mailbox.Receive ()
                let state = match msg with
                            | SetLocation (loc) -> let state = { config with Location = loc }
                                                   let query = sprintf "INSERT INTO sensorstate (fieldid, nodeid, sensorid, latitude, longitude) VALUES ('%s', '%s', '%s', %f, %f)" fieldid nodeid sensorid <|| (let (Spine.Model.Location(lat,lng)) = state.Location in lat, lng)
                                                   Spine.Configuration.Session.Execute(query) |> ignore
                                                   state
                            | Measurement(reading) -> let (Spine.Model.Location (lat,lng)) = config.Location
                                                      let columns = ["time"; "value"; "sensortype"]
                                                      let values = [ box reading.Timestamp; box reading.Value; config.SensorType :> obj]
                                                      let points = [{ name = seriesName; columns = columns; points = [values] }]
                                                                   |> Newtonsoft.Json.JsonConvert.SerializeObject
                                                      let resp = createRequest Post "http://178.62.84.55:8086/db/sensorsaurus/series?u=root&p=root&time_precision=s"
                                                                 |> withBody points
                                                                 |> getResponse
                                                      { config with LastContacted = System.DateTime.UtcNow }
                                                               
                return! loop state
            }
            printfn "Spawning sensor of id: %s/%s/%s" fieldid nodeid sensorid
            let config = LoadSensorStateFromCassandra fieldid nodeid sensorid
            let config = if config.SensorType = "" then 
                             let points = [{ name = seriesName; columns = ["time"; "value"; "sensortype"]; points = [[box 0 ; box 0.0; box sensorType]] }]
                                                                   |> Newtonsoft.Json.JsonConvert.SerializeObject
                             let resp = createRequest Post "http://178.62.84.55:8086/db/sensorsaurus/series?u=root&p=root&time_precision=s"
                                        |> withBody points
                                        |> getResponse
                             { config with SensorType = sensorType } 
                         else config
            loop config
        
        let NodeActor (mailbox:Actor<NodeMessage>) =
            let nodeid = mailbox.Self.Path.Name
            let fieldid = mailbox.Self.Path.Parent.Name
            let rec loop state = actor {
                let! msg = mailbox.Receive ()
                let state = match msg with
                            | NodeMessage.SensorsAvailable(sensors) -> let children = mailbox.Context.GetChildren()
                                                                                      |> Seq.map (fun t -> t.Path.Name)
                                                                                      |> Set.ofSeq
                                                                       let s = sensors.Sensors
                                                                               |> Seq.map (fun t -> t.SensorId, t.SensorType)
                                                                               |> Map.ofSeq
                                                                       let sensors = sensors.Sensors
                                                                                     |> Seq.map (fun t -> t.SensorId)
                                                                                     |> Set.ofSeq
                                                                       let toSpawn = Set.difference sensors children
                                                                                     |> Set.map (fun t -> { SensorId = t; SensorType = s.[t] })

                                                                       printfn "Spawning: %A" toSpawn
                                                                       toSpawn
                                                                       |> Set.iter (fun t -> mailbox.spawn t.SensorId (SensorActor t.SensorType) |> ignore
                                                                                             let query = sprintf "INSERT INTO sensorstate (fieldid, nodeid, sensorid, sensortype, latitude, longitude) VALUES('%s', '%s', '%s', '%s', %f, %f)" fieldid nodeid t.SensorId t.SensorType <|| (let (Spine.Model.Location(lat,lng)) = state.Location in lat,lng)
                                                                                             Spine.Configuration.Session.Execute(query) |> ignore)
                                                                       state
                            | NodeMessage.SetBatteryStatus(status) -> { state with BatteryStatus = status }
                            | NodeMessage.SetLocation(location) -> mailbox.Context.GetChildren()
                                                                   |> Seq.iter (fun t -> t <! SensorMessage.SetLocation(location))
                                                                   let state = { state with Location = location }
                                                                   let query = sprintf "INSERT INTO nodestate (fieldid, nodeid, latitude, longitude) VALUES ('%s', '%s', %f, %f)" fieldid nodeid <|| (let (Spine.Model.Location(lat, lng)) = state.Location in lat, lng)
                                                                   Spine.Configuration.Session.Execute(query) |> ignore
                                                                   state
                return! loop state
            }
            let nodeid = mailbox.Self.Path.Name
            let fieldid = mailbox.Self.Path.Parent.Name

            let query = sprintf "SELECT sensorid, sensortype FROM sensorstate WHERE fieldid = '%s' and nodeid = '%s'" fieldid nodeid
            let results = Spine.Configuration.Session.Execute(query) |> Seq.toList
            results
            |> Seq.iter (fun t -> let sensor = t.GetValue<string>("sensorid")
                                  let sensorType = t.GetValue<string>("sensortype")
                                  mailbox.spawn sensor (SensorActor sensorType) |> ignore)

            let config = LoadNodeStateFromCassandra fieldid nodeid

            loop config

        let FieldActor (mailbox:Actor<FieldMessage>) =
            //TODO: Implement the field actor messages
            let rec loop state = actor {
                let! msg = mailbox.Receive ()
                match msg with
                | FieldMessage.AddNode(location, id) -> let actor = mailbox.spawn id NodeActor
                                                        actor <! NodeMessage.SetLocation(location)
                return! loop state
            }
            let fieldid = mailbox.Self.Path.Name

            printfn "Loading a field"

            let query = sprintf "SELECT nodeid FROM nodestate WHERE fieldid = '%s'" fieldid
            let results = Spine.Configuration.Session.Execute(query) |> Seq.toList
            results 
            |> Seq.iter (fun t -> let node = t.GetValue<string>("nodeid")
                                  mailbox.spawn node NodeActor |> ignore)

            loop ()

namespace Spine

module ActorSystem =

    open Akka.Actor

    let System = ActorSystem.Create("sensorsaurus")