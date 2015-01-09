// Learn more about F# at http://fsharp.net
// See the 'F# Tutorial' project for more help.

open Akka.Actor
open Nancy.Hosting.Self
open System
open Spine.ApiConfiguration
open Spine.Actors
open Spine.Model
open Akka.FSharp

[<EntryPoint>]
let main argv = 

    let system = Spine.ActorSystem.System

    let fields = Spine.Configuration.Session.Execute("SELECT * FROM fieldstate")
                 |> Seq.iter (fun t -> let fieldid = t.GetValue<string>("fieldid")
                                       spawn system fieldid FieldActor |> ignore)

    System.Threading.Thread.Sleep 10000

    let uri = "http://localhost:8888"

    use host = new NancyHost(new Uri("http://178.62.84.55:8888"))
    host.Start()

    printfn "Started the web service"
    System.Threading.Thread.Sleep(System.Threading.Timeout.Infinite) |> ignore
    0 // return an integer exit code
